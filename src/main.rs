use warp::{Filter};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Debug};
use std::fmt;
use serde::export::Formatter;
use bollard::Docker;
use std::default::Default;
use std::sync::Arc;
use bollard::exec::CreateExecOptions;
use bollard::container::{CreateContainerOptions, StartContainerOptions, Config, CreateContainerResults, ListContainersOptions, HostConfig};

#[derive(Deserialize)]
struct ContainerStorageConfiguration {
    host: String,
    local: String,
    ro: bool
}

#[derive(Deserialize)]
struct ContainerConfiguration {
    image: String,
    name: String,
    hostname: Option<String>,
    domain: Option<String>,
    ports: Option<Vec<u16>>,
    storage: Option<Vec<ContainerStorageConfiguration>>,
    environment: Option<HashMap<String, String>>
}

#[derive(Debug)]
enum StartFailureReason {
    StoragePathDNE,
    ImageDNE,
    PortBindFailure,
    PermissionDenied,
    Other
}

#[derive(Debug)]
struct ContainerRunError {
    message: String,
    reason: StartFailureReason,
}

impl Error for ContainerRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl std::convert::From<bollard::errors::Error> for ContainerRunError {
    fn from(err: bollard::errors::Error) -> Self {
        let parts = match err.kind() {
            bollard::errors::ErrorKind::HyperResponseError { err: e } => {
                match e.source() {
                    Some(e) => {
                        if format!("{}", e).to_lowercase().contains("permission denied") {
                            (StartFailureReason::PermissionDenied, format!("{}", e.description()))
                        } else {
                            (StartFailureReason::Other, format!("{}", e))
                        }
                    },
                    None => (StartFailureReason::Other, format!("{}", e))
                }
            }
            _ => (StartFailureReason::Other, format!("{}", err))
        };
        ContainerRunError { message: parts.1, reason: parts.0 }
    }
}

impl warp::reject::Reject for ContainerRunError {}

impl Display for ContainerRunError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.reason {
            StartFailureReason::PermissionDenied => write!(f, "Permission denied communicating with docker service: {}", &self.message),
            _ => write!(f, "Error occurred")
        }
    }
}

#[tokio::main]
async fn main() {
    let container_platform = "docker";
    let runner = match container_platform {
        "docker" => {
            let docker = Docker::connect_with_local_defaults().unwrap();
            DockerRunner::new(docker)
        }
        _ => {
            unreachable!();
        }
    };
    let post = warp::post();
    let run = warp::path!("run").
        and(warp::body::json()).
        and(with_runner(runner.clone())).
        and_then( |conf: ContainerConfiguration, runner: DockerRunner| async move {
            // TODO JSON format and status code on errors
            match runner.run_container(conf).await {
                Ok(()) => Ok(format!("Started successfully")),
                Err(error) => {
                    Err(warp::reject::custom(error))
                }
            }
        });
    warp::serve(post.and(run)).run(([127, 0, 0, 1], 3030)).await;
}

#[derive(Clone, Debug)]
struct DockerRunner {
    docker: Arc<Docker>
}

fn with_runner(runner: DockerRunner) -> impl Filter<Extract = (DockerRunner,), Error = std::convert::Infallible> + Clone {
    // TODO make this take a trait implementing generic type once we figure out how to get around the no-async trait issue
    warp::any().map(move || runner.clone())
}


impl DockerRunner {
    fn new(docker: Docker) -> Self {
        return DockerRunner {
            docker: Arc::new(docker)
        }
    }

    async fn find_container(&self, conf: &ContainerConfiguration) -> Result<Option<String>, bollard::errors::Error> {
        let result = self.docker.list_containers(Some(ListContainersOptions::<String>{all: true, ..Default::default()})).await;
        match result {
            Ok(list_results) => {
                let mut name = "/".to_owned();
                name.push_str(&conf.name);
                match list_results.into_iter().filter(|result| {
                    // TODO what do we do if the name exists but it's for a different image?
                    result.names.contains(&name)
                }).nth(0) {
                    Some(image) => Ok(Some(image.id)),
                    None => Ok(None)
                }
            }
            Err(err) => {
                Err(err)
            }
        }
    }

    async fn create_container(&self, conf: &ContainerConfiguration) -> Result<CreateContainerResults, bollard::errors::Error> {
        let create_opts = Some(CreateContainerOptions{
            name: conf.name.to_owned()
        });
        let host_config = if let Some(storage) = &conf.storage {
            Some(HostConfig{
                binds: Some(storage.into_iter().fold(Vec::new(), | mut v, vol| {
                    let mut host_path = vol.host.clone();
                    let local_path = vol.local.clone();
                    host_path.push_str(&local_path);
                    if vol.ro { host_path.push_str(":ro")};
                    v.push(host_path);
                    v
                })),
                ..Default::default()
            })
        } else {None};
        let create_config = Config{
            image: Some(conf.image.to_owned()),
            env: match &conf.environment {
                Some(hm) => {
                    let mut vec: Vec<String> = Vec::new();
                    for (k, v) in hm.iter() {
                        vec.push(format!("{}={}", k, v));
                    }
                    Some(vec)
                },
                None => None
            },
            domainname: conf.domain.clone(),
            hostname: conf.hostname.clone(),
            host_config,
            ..Default::default()
        };
        self.docker.create_container(create_opts, create_config).await
    }

    /**
    create a container if one does not exist, and then start it, based on provided configuration
    */
    async fn run_container(&self, conf: ContainerConfiguration) -> Result<(), ContainerRunError> {
        let found = self.find_container(&conf).await?;
        // TODO if found and running, probably raise an error
        // TODO if found and not running, may need to update config (env, etc)
        let container_id = match found {
            Some(id) => id,
            None => self.create_container(&conf).await?.id
        };
        Ok(self.docker.start_container(&container_id, None::<StartContainerOptions<String>>).await?)
    }
}
