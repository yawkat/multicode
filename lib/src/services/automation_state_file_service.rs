use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::time::{MissedTickBehavior, interval};

use super::{
    root_session_service::RootSessionStatus, runtime::automation_state_file_source,
    workspace_watch::monitor_workspace_snapshots,
};
use crate::{
    AutomationAgentState, WorkspaceManager, WorkspaceManagerError, WorkspaceSnapshot,
    manager::Workspace,
};

const STATE_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub enum AutomationStateFileServiceError {
    Manager(WorkspaceManagerError),
}

impl From<WorkspaceManagerError> for AutomationStateFileServiceError {
    fn from(value: WorkspaceManagerError) -> Self {
        Self::Manager(value)
    }
}

pub async fn automation_state_file_service(
    manager: Arc<WorkspaceManager>,
    workspace_directory_path: PathBuf,
) -> Result<(), AutomationStateFileServiceError> {
    monitor_workspace_snapshots(manager, move |key, workspace, workspace_rx| {
        let workspace_directory_path = workspace_directory_path.clone();
        async move {
            tokio::spawn(async move {
                watch_workspace(
                    workspace,
                    workspace_rx,
                    automation_state_file_source(&workspace_directory_path, &key),
                )
                .await;
            });
            Ok(())
        }
    })
    .await
}

async fn watch_workspace(
    workspace: Workspace,
    mut workspace_rx: tokio::sync::watch::Receiver<WorkspaceSnapshot>,
    state_file: PathBuf,
) {
    let mut refresh = interval(STATE_REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let snapshot = workspace_rx.borrow().clone();
        let should_track = snapshot.transient.is_some()
            && !snapshot.persistent.archived
            && !snapshot.persistent.automation_paused
            && active_task_id_for_snapshot(&snapshot).is_some();

        if should_track {
            apply_state_file_snapshot(&workspace, read_state_file(&state_file).await);
        } else {
            clear_automation_state(&workspace);
        }

        tokio::select! {
            changed = workspace_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            _ = refresh.tick() => {}
        }
    }
}

fn apply_state_file_snapshot(workspace: &Workspace, next: Option<ParsedAutomationState>) {
    workspace.update(|snapshot| {
        let resolved_active_task_id = active_task_id_for_snapshot(snapshot);
        let Some(target_task_id) =
            state_update_target_task_id(snapshot, resolved_active_task_id.as_deref(), next.as_ref())
        else {
            return if next.is_some() {
                false
            } else {
                clear_automation_state_snapshot(snapshot)
            };
        };
        if next.is_none()
            && expected_session_id_for_active_task(snapshot, &target_task_id).is_some()
        {
            return false;
        }
        let next_session_id = next.as_ref().and_then(|state| state.thread_id.clone());
        let next_agent_state = next.as_ref().map(|state| state.state);
        let next_session_status = next.as_ref().map(|state| state.state.root_status());
        let should_update_bridge_state =
            resolved_active_task_id.as_deref() == Some(target_task_id.as_str())
                || resolved_active_task_id.is_none();

        let mut changed = false;
        if should_update_bridge_state {
            if snapshot.automation_session_id != next_session_id {
                snapshot.automation_session_id = next_session_id.clone();
                changed = true;
            }
            if snapshot.automation_agent_state != next_agent_state {
                snapshot.automation_agent_state = next_agent_state;
                changed = true;
            }
            if snapshot.automation_session_status != next_session_status {
                snapshot.automation_session_status = next_session_status;
                changed = true;
            }
            if snapshot.active_task_id.is_none()
                && snapshot.active_task_id.as_deref() != Some(target_task_id.as_str())
            {
                snapshot.active_task_id = Some(target_task_id.clone());
                changed = true;
            }
        }
        let task_state = snapshot.task_states.entry(target_task_id).or_default();
        if task_state.session_id != next_session_id {
            task_state.session_id = next_session_id.clone();
            changed = true;
        }
        if task_state.agent_state != next_agent_state {
            task_state.agent_state = next_agent_state;
            changed = true;
        }
        if task_state.session_status != next_session_status {
            task_state.session_status = next_session_status;
            changed = true;
        }
        if task_state.waiting_on_vm {
            task_state.waiting_on_vm = false;
            changed = true;
        }
        changed
    });
}

fn clear_automation_state(workspace: &Workspace) {
    workspace.update(clear_automation_state_snapshot);
}

fn clear_automation_state_snapshot(snapshot: &mut WorkspaceSnapshot) -> bool {
    let mut changed = false;
    if let Some(active_task_id) = active_task_id_for_snapshot(snapshot)
        && let Some(task_state) = snapshot.task_states.get_mut(&active_task_id)
    {
        if task_state.session_id.take().is_some() {
            changed = true;
        }
        if task_state.agent_state.take().is_some() {
            changed = true;
        }
        if task_state.session_status.take().is_some() {
            changed = true;
        }
        if task_state.waiting_on_vm {
            task_state.waiting_on_vm = false;
            changed = true;
        }
    }
    if snapshot.automation_session_id.take().is_some() {
        changed = true;
    }
    if snapshot.automation_agent_state.take().is_some() {
        changed = true;
    }
    if snapshot.automation_session_status.take().is_some() {
        changed = true;
    }
    changed
}

fn active_task_id_for_snapshot(snapshot: &WorkspaceSnapshot) -> Option<String> {
    snapshot.resolved_active_task_id()
}

fn state_update_target_task_id(
    snapshot: &WorkspaceSnapshot,
    resolved_active_task_id: Option<&str>,
    next: Option<&ParsedAutomationState>,
) -> Option<String> {
    if let Some(thread_id) = next.and_then(|state| state.thread_id.as_deref()) {
        if let Some(task_id) = task_id_for_session_id(snapshot, thread_id) {
            return Some(task_id);
        }
        if let Some(active_task_id) = resolved_active_task_id
        {
            let expected_session_id = expected_session_id_for_active_task(snapshot, active_task_id);
            if expected_session_id.is_none() || expected_session_id == Some(thread_id) {
                return Some(active_task_id.to_string());
            }
        }
        return None;
    }

    resolved_active_task_id.map(ToOwned::to_owned)
}

fn expected_session_id_for_active_task<'a>(
    snapshot: &'a WorkspaceSnapshot,
    active_task_id: &str,
) -> Option<&'a str> {
    snapshot
        .task_states
        .get(active_task_id)
        .and_then(|task_state| task_state.session_id.as_deref())
        .or(snapshot.automation_session_id.as_deref())
}

fn task_id_for_session_id(snapshot: &WorkspaceSnapshot, session_id: &str) -> Option<String> {
    snapshot.task_states.iter().find_map(|(task_id, task_state)| {
        (task_state.session_id.as_deref() == Some(session_id)).then(|| task_id.clone())
    })
}

async fn read_state_file(path: &Path) -> Option<ParsedAutomationState> {
    tokio::fs::metadata(path).await.ok()?;
    let contents = tokio::fs::read_to_string(path).await.ok()?;
    parse_state_file(&contents)
}

fn parse_state_file(contents: &str) -> Option<ParsedAutomationState> {
    let trimmed = contents.trim();
    let (state, thread_id) =
        trimmed
            .split_once(':')
            .map_or((trimmed, None), |(state, thread_id)| {
                let thread_id = thread_id.trim();
                (
                    state.trim(),
                    (!thread_id.is_empty()).then(|| thread_id.to_string()),
                )
            });

    let state = if state.eq_ignore_ascii_case("working") {
        AutomationAgentState::Working
    } else if state.eq_ignore_ascii_case("question") {
        AutomationAgentState::Question
    } else if state.eq_ignore_ascii_case("review") {
        AutomationAgentState::Review
    } else if state.eq_ignore_ascii_case("idle") {
        AutomationAgentState::Idle
    } else {
        AutomationAgentState::Stale
    };

    Some(ParsedAutomationState { state, thread_id })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedAutomationState {
    state: AutomationAgentState,
    thread_id: Option<String>,
}

impl AutomationAgentState {
    fn root_status(self) -> RootSessionStatus {
        match self {
            AutomationAgentState::Working => RootSessionStatus::Busy,
            AutomationAgentState::WaitingOnVm => RootSessionStatus::Idle,
            AutomationAgentState::Question => RootSessionStatus::Question,
            AutomationAgentState::Review
            | AutomationAgentState::Idle
            | AutomationAgentState::Stale => RootSessionStatus::Idle,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkspaceTaskPersistentSnapshot, WorkspaceTaskSource};

    #[test]
    fn parse_state_file_maps_known_states() {
        let parsed = parse_state_file("question:thread-123\n").expect("state exists");

        assert_eq!(parsed.state, AutomationAgentState::Question);
        assert_eq!(parsed.thread_id.as_deref(), Some("thread-123"));
    }

    #[test]
    fn parse_state_file_marks_unknown_state_as_stale() {
        let parsed = parse_state_file("bogus\n").expect("state exists");

        assert_eq!(parsed.state, AutomationAgentState::Stale);
    }

    #[test]
    fn active_task_id_falls_back_to_automation_issue_mapping() {
        let mut snapshot = WorkspaceSnapshot::default();
        snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
            "task-42".to_string(),
            "https://github.com/example/repo/issues/42".to_string(),
            WorkspaceTaskSource::Manual,
        ));
        snapshot.persistent.automation_issue =
            Some("https://github.com/example/repo/issues/42".to_string());

        assert_eq!(
            active_task_id_for_snapshot(&snapshot).as_deref(),
            Some("task-42")
        );
    }

    #[test]
    fn apply_state_file_snapshot_populates_task_state_for_fallback_active_task() {
        let workspace = Workspace::new(WorkspaceSnapshot::default());
        workspace.update(|snapshot| {
            snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
                "task-42".to_string(),
                "https://github.com/example/repo/issues/42".to_string(),
                WorkspaceTaskSource::Manual,
            ));
            snapshot.persistent.automation_issue =
                Some("https://github.com/example/repo/issues/42".to_string());
            true
        });

        apply_state_file_snapshot(
            &workspace,
            Some(ParsedAutomationState {
                state: AutomationAgentState::Working,
                thread_id: Some("thread-42".to_string()),
            }),
        );

        let snapshot = workspace.subscribe().borrow().clone();
        assert_eq!(snapshot.active_task_id.as_deref(), Some("task-42"));
        assert_eq!(snapshot.automation_session_id.as_deref(), Some("thread-42"));
        assert_eq!(
            snapshot.automation_agent_state,
            Some(AutomationAgentState::Working)
        );
        let task_state = snapshot
            .task_states
            .get("task-42")
            .expect("task state should be created");
        assert_eq!(task_state.session_id.as_deref(), Some("thread-42"));
        assert_eq!(task_state.agent_state, Some(AutomationAgentState::Working));
    }

    #[test]
    fn apply_state_file_snapshot_ignores_explicit_mismatched_session_id() {
        let workspace = Workspace::new(WorkspaceSnapshot::default());
        workspace.update(|snapshot| {
            snapshot.active_task_id = Some("task-42".to_string());
            snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
                "task-42".to_string(),
                "https://github.com/example/repo/issues/42".to_string(),
                WorkspaceTaskSource::Manual,
            ));
            snapshot.task_states.insert(
                "task-42".to_string(),
                crate::WorkspaceTaskRuntimeSnapshot {
                    session_id: Some("thread-42".to_string()),
                    agent_state: Some(AutomationAgentState::Working),
                    ..Default::default()
                },
            );
            snapshot.automation_session_id = Some("thread-42".to_string());
            snapshot.automation_agent_state = Some(AutomationAgentState::Working);
            true
        });

        apply_state_file_snapshot(
            &workspace,
            Some(ParsedAutomationState {
                state: AutomationAgentState::Review,
                thread_id: Some("thread-old".to_string()),
            }),
        );

        let snapshot = workspace.subscribe().borrow().clone();
        assert_eq!(snapshot.automation_session_id.as_deref(), Some("thread-42"));
        assert_eq!(
            snapshot.automation_agent_state,
            Some(AutomationAgentState::Working)
        );
        let task_state = snapshot
            .task_states
            .get("task-42")
            .expect("task state should remain");
        assert_eq!(task_state.session_id.as_deref(), Some("thread-42"));
        assert_eq!(task_state.agent_state, Some(AutomationAgentState::Working));
    }

    #[test]
    fn apply_state_file_snapshot_preserves_existing_session_when_state_file_missing() {
        let workspace = Workspace::new(WorkspaceSnapshot::default());
        workspace.update(|snapshot| {
            snapshot.active_task_id = Some("task-42".to_string());
            snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
                "task-42".to_string(),
                "https://github.com/example/repo/issues/42".to_string(),
                WorkspaceTaskSource::Manual,
            ));
            snapshot.task_states.insert(
                "task-42".to_string(),
                crate::WorkspaceTaskRuntimeSnapshot {
                    session_id: Some("thread-42".to_string()),
                    agent_state: Some(AutomationAgentState::Working),
                    session_status: Some(RootSessionStatus::Busy),
                    ..Default::default()
                },
            );
            snapshot.automation_session_id = Some("thread-42".to_string());
            snapshot.automation_agent_state = Some(AutomationAgentState::Working);
            snapshot.automation_session_status = Some(RootSessionStatus::Busy);
            true
        });

        apply_state_file_snapshot(&workspace, None);

        let snapshot = workspace.subscribe().borrow().clone();
        assert_eq!(snapshot.automation_session_id.as_deref(), Some("thread-42"));
        assert_eq!(
            snapshot.automation_agent_state,
            Some(AutomationAgentState::Working)
        );
        assert_eq!(
            snapshot.automation_session_status,
            Some(RootSessionStatus::Busy)
        );
        let task_state = snapshot
            .task_states
            .get("task-42")
            .expect("task state should remain");
        assert_eq!(task_state.session_id.as_deref(), Some("thread-42"));
        assert_eq!(task_state.agent_state, Some(AutomationAgentState::Working));
        assert_eq!(task_state.session_status, Some(RootSessionStatus::Busy));
    }

    #[test]
    fn apply_state_file_snapshot_updates_matching_task_without_reassigning_active_lease() {
        let workspace = Workspace::new(WorkspaceSnapshot::default());
        workspace.update(|snapshot| {
            snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
                "task-42".to_string(),
                "https://github.com/example/repo/issues/42".to_string(),
                WorkspaceTaskSource::Manual,
            ));
            snapshot.persistent.tasks.push(WorkspaceTaskPersistentSnapshot::new(
                "task-30".to_string(),
                "https://github.com/example/repo/issues/30".to_string(),
                WorkspaceTaskSource::Manual,
            ));
            snapshot.active_task_id = Some("task-30".to_string());
            snapshot.persistent.automation_issue =
                Some("https://github.com/example/repo/issues/30".to_string());
            snapshot.task_states.insert(
                "task-42".to_string(),
                crate::WorkspaceTaskRuntimeSnapshot {
                    session_id: Some("thread-42".to_string()),
                    agent_state: Some(AutomationAgentState::Working),
                    session_status: Some(RootSessionStatus::Busy),
                    ..Default::default()
                },
            );
            snapshot.task_states.insert(
                "task-30".to_string(),
                crate::WorkspaceTaskRuntimeSnapshot {
                    session_id: Some("thread-30".to_string()),
                    agent_state: Some(AutomationAgentState::Working),
                    session_status: Some(RootSessionStatus::Busy),
                    ..Default::default()
                },
            );
            snapshot.automation_session_id = Some("thread-30".to_string());
            snapshot.automation_agent_state = Some(AutomationAgentState::Working);
            snapshot.automation_session_status = Some(RootSessionStatus::Busy);
            true
        });

        apply_state_file_snapshot(
            &workspace,
            Some(ParsedAutomationState {
                state: AutomationAgentState::Review,
                thread_id: Some("thread-42".to_string()),
            }),
        );

        let snapshot = workspace.subscribe().borrow().clone();
        assert_eq!(snapshot.active_task_id.as_deref(), Some("task-30"));
        assert_eq!(snapshot.automation_session_id.as_deref(), Some("thread-30"));
        assert_eq!(
            snapshot.automation_agent_state,
            Some(AutomationAgentState::Working)
        );
        let task_42 = snapshot
            .task_states
            .get("task-42")
            .expect("task 42 should remain");
        assert_eq!(task_42.session_id.as_deref(), Some("thread-42"));
        assert_eq!(task_42.agent_state, Some(AutomationAgentState::Review));
        assert_eq!(task_42.session_status, Some(RootSessionStatus::Idle));
        let task_30 = snapshot
            .task_states
            .get("task-30")
            .expect("task 30 should remain");
        assert_eq!(task_30.session_id.as_deref(), Some("thread-30"));
        assert_eq!(task_30.agent_state, Some(AutomationAgentState::Working));
    }
}
