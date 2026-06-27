use super::*;

pub fn workflow_database_path(sporos_database_path: &Path) -> PathBuf {
    let parent = sporos_database_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let state_root = if parent
        .file_name()
        .is_some_and(|name| name == DEFAULT_DATABASE_DIR)
    {
        parent.parent().unwrap_or(parent)
    } else {
        parent
    };
    state_root.join(WORKFLOW_DATABASE_FILE)
}

pub fn workflow_runtime_dependency_name() -> Result<DependencyName, DuroxideWorkflowRuntimeError> {
    DependencyName::new(WORKFLOW_RUNTIME_DEPENDENCY).map_err(|error| {
        DuroxideWorkflowRuntimeError::InvalidDependencyName {
            message: error.to_string(),
        }
    })
}

pub(super) async fn prepare_workflow_database(
    path: &Path,
) -> Result<(), DuroxideWorkflowRuntimeError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            DuroxideWorkflowRuntimeError::PrepareDatabase {
                path: path.to_path_buf(),
                message: error.to_string(),
            }
        })?;
    }
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|error| DuroxideWorkflowRuntimeError::PrepareDatabase {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    Ok(())
}
