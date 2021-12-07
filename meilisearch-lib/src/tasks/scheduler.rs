use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{hash_map::Entry, BinaryHeap, HashMap, VecDeque},
    rc::Rc,
};

use chrono::Utc;

use super::{
    batch::Batch,
    task::{Task, TaskContent, TaskId},
    task_store::TaskStore,
    Pending,
};

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
enum TaskType {
    DocumentAddition,
    Other,
}

#[derive(Eq)]
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
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for PendingTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Ord, Eq)]
struct TaskList {
    index: String,
    tasks: VecDeque<PendingTask>,
}

impl TaskList {
    fn new(index: String) -> Self {
        Self {
            index,
            tasks: VecDeque::new(),
        }
    }

    fn push(&mut self, id: PendingTask) {
        self.tasks.push_front(id);
    }

    fn peek(&self) -> Option<&PendingTask> {
        self.tasks.back()
    }
}

impl PartialEq for TaskList {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl PartialOrd for TaskList {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.peek(), other.peek()) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) => Some(Ordering::Greater),
            (Some(_), None) => Some(Ordering::Less),
            (Some(lhs), Some(rhs)) => Some(lhs.cmp(&rhs).reverse()),
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
                entry.get().borrow_mut().push(task);
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

    /// passes a context with a view to the task list of the next index to schedule. It is
    /// guaranteed that the first id from task list will be the lowest pending task id.
    fn head_mut(&mut self, mut f: impl FnMut(&mut TaskList)) {
        if let Some(head) = self.queue.pop() {
            {
                let mut ref_head = head.borrow_mut();
                f(&mut *ref_head);
            }
            if !head.borrow().tasks.is_empty() {
                // After being mutated, the head is reinserted to the correct position.
                self.queue.push(head);
            } else {
                self.index_tasks.remove(dbg!(&head.borrow().index));
            }
        }
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
}

struct Scheduler {
    pending_queue: PendingQueue,
    store: TaskStore,
}

impl Scheduler {
    fn prepare_batch(&mut self) -> Batch {
        if let Some(Pending::Job(job)) = self.pending_queue.jobs.pop_back() {
            return Batch {
                id: 0,
                created_at: Utc::now(),
                tasks: vec![Pending::Job(job)],
            };
        }

        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::{index_resolver::IndexUid, tasks::task::TaskContent};

    use super::*;

    fn gen_task(id: TaskId, index_uid: &str) -> Task {
        Task {
            id,
            index_uid: IndexUid::new_unchecked(index_uid.to_owned()),
            content: TaskContent::IndexDeletion,
            events: vec![],
        }
    }

    #[test]
    fn register_updates_multiples_indexes() {
        let mut queue = TaskQueue::default();
        queue.insert(gen_task(0, "test1"));
        queue.insert(gen_task(1, "test2"));
        queue.insert(gen_task(2, "test2"));
        queue.insert(gen_task(3, "test2"));
        queue.insert(gen_task(4, "test1"));
        queue.insert(gen_task(5, "test1"));
        queue.insert(gen_task(6, "test2"));

        let mut test1_tasks = Vec::new();
        queue.head_mut(|tasks| {
            while let Some(task) = tasks.tasks.pop_back() {
                test1_tasks.push(task.id);
            }
            assert!(tasks.tasks.is_empty());
        });

        assert_eq!(test1_tasks, &[0, 4, 5]);

        let mut test2_tasks = Vec::new();
        queue.head_mut(|tasks| {
            while let Some(task) = tasks.tasks.pop_back() {
                test2_tasks.push(task.id);
            }
            assert!(tasks.tasks.is_empty());
        });

        assert_eq!(test2_tasks, &[1, 2, 3, 6]);

        assert!(queue.index_tasks.is_empty());
        assert!(queue.queue.is_empty());
    }
}
