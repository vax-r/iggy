use super::NoShutdown;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::task_registry::registry::TaskRegistry;
use iggy_common::IggyError;
use std::ops::AsyncFnOnce;
use std::time::Duration;

pub struct OneShotBuilder<'a, Task, OnShutdown = NoShutdown> {
    reg: &'a TaskRegistry,
    name: &'static str,
    critical: bool,
    timeout: Option<Duration>,
    run_fn: Option<Task>,
    on_shutdown: Option<OnShutdown>,
}

impl<'a> OneShotBuilder<'a, (), NoShutdown> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            critical: false,
            timeout: None,
            run_fn: None,
            on_shutdown: None,
        }
    }
}

impl<'a, Task, OnShutdown> OneShotBuilder<'a, Task, OnShutdown> {
    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn on_shutdown<NewShutdown>(self, f: NewShutdown) -> OneShotBuilder<'a, Task, NewShutdown>
    where
        NewShutdown: AsyncFnOnce(Result<(), IggyError>) + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            timeout: self.timeout,
            run_fn: self.run_fn,
            on_shutdown: Some(f),
        }
    }
}

impl<'a, OnShutdown> OneShotBuilder<'a, (), OnShutdown> {
    pub fn run<NewTask>(self, f: NewTask) -> OneShotBuilder<'a, NewTask, OnShutdown>
    where
        NewTask: AsyncFnOnce(ShutdownToken) -> Result<(), IggyError> + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            timeout: self.timeout,
            run_fn: Some(f),
            on_shutdown: self.on_shutdown,
        }
    }
}

impl<'a, Task> OneShotBuilder<'a, Task, NoShutdown>
where
    Task: AsyncFnOnce(ShutdownToken) -> Result<(), IggyError> + 'static,
{
    pub fn spawn(self) {
        let run_fn = self.run_fn.expect("run() must be called before spawn()");
        self.reg.spawn_oneshot_closure(
            self.name,
            self.critical,
            self.timeout,
            run_fn,
            Some(|_| async {}),
        );
    }
}

impl<'a, Task, OnShutdown> OneShotBuilder<'a, Task, OnShutdown>
where
    Task: AsyncFnOnce(ShutdownToken) -> Result<(), IggyError> + 'static,
    OnShutdown: AsyncFnOnce(Result<(), IggyError>) + 'static,
{
    pub fn spawn(self) {
        let run_fn = self.run_fn.expect("run() must be called before spawn()");
        self.reg.spawn_oneshot_closure(
            self.name,
            self.critical,
            self.timeout,
            run_fn,
            self.on_shutdown,
        );
    }
}
