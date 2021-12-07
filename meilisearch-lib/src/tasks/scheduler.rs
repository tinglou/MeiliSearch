use std::{
    cell::{Ref, RefCell},
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque},
    ops::{Deref, DerefMut},
    rc::Rc,
};

use chrono::Utc;

use super::{
    batch::Batch,
    error::Result,
    task::{Task, TaskContent, TaskId},
    task_store::TaskStore,
    Pending,
};

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
enum TaskType {
    DocumentAddition,
    Other,
}

#[derive(Eq, Debug, Clone, Copy)]
struct PendingTask {
    kind: TaskType,
    id: TaskId,
}

impl PartialEq for PendingTask {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl PartialOrd for PendingTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id).map(Ordering::reverse)
    }
}

impl Ord for PendingTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug)]
struct TaskList {
    index: String,
    tasks: BinaryHeap<PendingTask>,
}

impl Deref for TaskList {
    type Target = BinaryHeap<PendingTask>;

    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}

impl DerefMut for TaskList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tasks
    }
}

impl TaskList {
    fn new(index: String) -> Self {
        Self {
            index,
            tasks: Default::default(),
        }
    }
}

impl PartialEq for TaskList {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for TaskList {}
impl Ord for TaskList {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for TaskList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.peek(), other.peek()) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (Some(lhs), Some(rhs)) => Some(lhs.cmp(&rhs)),
        }
    }
}

#[derive(Default)]
struct TaskQueue {
    /// maps index uids to their TaskList, for quick access
    index_tasks: HashMap<String, Rc<RefCell<TaskList>>>,
    /// A queue that orders TaskList by the priority of their fist update
    queue: BinaryHeap<Rc<RefCell<TaskList>>>,
}

impl TaskQueue {
    fn insert(&mut self, task: Task) {
        let uid = task.index_uid.into_inner();
        let id = task.id;
        let kind = match task.content {
            TaskContent::DocumentAddition { .. } => TaskType::DocumentAddition,
            _ => TaskType::Other,
        };
        let task = PendingTask { kind, id };

        match self.index_tasks.entry(uid) {
            Entry::Occupied(entry) => {
                // task list already exists for this index, all we have to to is to push the new
                // update to the end of the list. This won't change the order since ids are
                // monotically increasing.
                let mut list = entry.get().borrow_mut();

                // in reality, we only need the first element to be lower than the one we want to
                // insert to preserve the order in the queue.
                assert!(list.peek().map(|old_id| id > old_id.id).unwrap_or(true));

                list.push(task);
            }
            Entry::Vacant(entry) => {
                let mut task_list = TaskList::new(entry.key().to_owned());
                task_list.push(task);
                let task_list = Rc::new(RefCell::new(task_list));
                entry.insert(task_list.clone());
                self.queue.push(task_list);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty() && self.index_tasks.is_empty()
    }

    fn head(&self) -> Option<Ref<TaskList>> {
        self.queue.peek().map(|t| t.borrow())
    }

    /// passes a context with a view to the task list of the next index to schedule. It is
    /// guaranteed that the first id from task list will be the lowest pending task id.
    fn head_mut<R>(&mut self, mut f: impl FnMut(&mut TaskList) -> R) -> Option<R> {
        let head = self.queue.pop()?;
        let result = {
            let mut ref_head = head.borrow_mut();
            f(&mut *ref_head)
        };
        if !head.borrow().tasks.is_empty() {
            // After being mutated, the head is reinserted to the correct position.
            self.queue.push(head);
        } else {
            self.index_tasks.remove(dbg!(&head.borrow().index));
        }

        Some(result)
    }
}

#[derive(Default)]
struct PendingQueue {
    jobs: VecDeque<Pending<TaskId>>,
    tasks: TaskQueue,
}

impl PendingQueue {
    fn register(&mut self, task: Pending<Task>) {
        match task {
            Pending::Task(t) => {
                self.tasks.insert(t);
            }
            Pending::Job(j) => {
                self.jobs.push_front(Pending::Job(j));
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.jobs.is_empty() && self.tasks.is_empty()
    }
}

struct Scheduler {
    pending_queue: PendingQueue,
    store: TaskStore,
    processing: Vec<TaskId>,
}

impl Scheduler {
    /// Prepares the next batch, and set `processing` to the ids in that batch.
    async fn prepare_batch(&mut self) -> Result<Option<Batch>> {
        self.processing.clear();
        if let Some(Pending::Job(job)) = self.pending_queue.jobs.pop_back() {
            Ok(Some(Batch {
                id: 0,
                created_at: Utc::now(),
                tasks: vec![Pending::Job(job)],
            }))
        } else {
            make_batch(&mut self.pending_queue, &mut self.processing);
            if !self.processing.is_empty() {
                let ids = std::mem::take(&mut self.processing);

                let (ids, tasks) = self.store.get_pending_tasks(ids).await?;

                self.processing = ids;
                let batch = Batch {
                    id: 0,
                    created_at: Utc::now(),
                    tasks,
                };

                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }
    }
}

fn make_batch(pending_queue: &mut PendingQueue, processing: &mut Vec<TaskId>) {
    pending_queue
        .tasks
        .head_mut(|list| match list.peek().copied() {
            Some(PendingTask {
                kind: TaskType::Other,
                id,
            }) => {
                processing.push(id);
                list.pop();
            }
            Some(PendingTask { kind, .. }) => loop {
                match list.peek() {
                    Some(pending) if pending.kind == kind => {
                        processing.push(pending.id);
                        list.pop();
                    }
                    _ => break,
                }
            },
            None => (),
        });
}

#[cfg(test)]
mod test {
    use milli::update::IndexDocumentsMethod;
    use uuid::Uuid;

    use crate::{index_resolver::IndexUid, tasks::task::TaskContent};

    use super::*;

    fn gen_task(id: TaskId, index_uid: &str, content: TaskContent) -> Task {
        Task {
            id,
            index_uid: IndexUid::new_unchecked(index_uid.to_owned()),
            content,
            events: vec![],
        }
    }

    #[test]
    fn register_updates_multiples_indexes() {
        let mut queue = TaskQueue::default();
        queue.insert(gen_task(0, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(1, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(2, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(3, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(4, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(5, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(6, "test2", TaskContent::IndexDeletion));

        let mut test1_tasks = queue
            .head_mut(|tasks| tasks.drain().map(|t| t.id).collect::<Vec<_>>())
            .unwrap();

        assert_eq!(test1_tasks, &[0, 4, 5]);

        let mut test2_tasks = queue
            .head_mut(|tasks| tasks.drain().map(|t| t.id).collect::<Vec<_>>())
            .unwrap();

        assert_eq!(test2_tasks, &[1, 2, 3, 6]);

        assert!(queue.index_tasks.is_empty());
        assert!(queue.queue.is_empty());
    }

    #[test]
    fn test_make_batch() {
        let mut queue = TaskQueue::default();
        let content = TaskContent::DocumentAddition {
            content_uuid: Uuid::new_v4(),
            merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
            primary_key: Some("test".to_string()),
            documents_count: 0,
        };
        queue.insert(gen_task(0, "test1", content.clone()));
        queue.insert(gen_task(1, "test2", content.clone()));
        queue.insert(gen_task(2, "test2", TaskContent::IndexDeletion));
        queue.insert(gen_task(3, "test2", content.clone()));
        queue.insert(gen_task(4, "test1", content.clone()));
        queue.insert(gen_task(5, "test1", TaskContent::IndexDeletion));
        queue.insert(gen_task(6, "test2", content.clone()));
        queue.insert(gen_task(7, "test1", content.clone()));

        let mut queue = PendingQueue {
            jobs: Default::default(),
            tasks: queue,
        };

        let mut batch = Vec::new();

        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[0, 4]);

        batch.clear();
        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[1]);

        batch.clear();
        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[2]);

        batch.clear();
        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[3, 6]);

        batch.clear();
        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[5]);

        batch.clear();
        make_batch(&mut queue, &mut batch);
        assert_eq!(batch, &[7]);

        assert!(queue.is_empty());
    }
}
