use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::PathBuf,
};

use anyhow::bail;
use itertools::Itertools;
use tiny_http::{Request, Response};

pub fn serve() {
    let server = tiny_http::Server::http("0.0.0.0:8000").unwrap();

    loop {
        let response = match server.recv() {
            Ok(rq) => {
                println!("request {:?}", rq.url());
                match rq
                    .url()
                    .strip_prefix("/")
                    .unwrap_or(rq.url())
                    .split('/')
                    .collect_vec()
                    .as_slice()
                {
                    ["list"] => to_response(rq, list_db_files()),
                    ["get", path] => {
                        let file = load_file(path);
                        to_response(rq, file)
                    }
                    _ => rq
                        .respond(Response::from_string("not found").with_status_code(404))
                        .map_err(Into::into),
                }
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        };
        if let Err(e) = response {
            println!("Error responding: {e}")
        }
    }
}

fn list_db_files() -> anyhow::Result<Response<Cursor<Vec<u8>>>> {
    let files = std::fs::read_dir("h3")?;
    let files = files
        .into_iter()
        .map(|l| match l {
            Ok(l) => l.file_name().to_string_lossy().to_string(),
            Err(e) => format!("Error {e}"),
        })
        .collect_vec();
    Ok(Response::from_string(serde_json::to_string(&files)?))
}

fn load_file(path: &str) -> anyhow::Result<Response<BufReader<File>>> {
    if path.contains("/") {
        bail!("no slashes in path names")
    }
    let root = PathBuf::from("h3");
    let f = std::fs::File::open(root.join(path))?;
    let r = Response::empty(200).with_data(BufReader::new(f), None);
    Ok(r)
}

fn to_response<R: Read>(
    request: Request,
    result: anyhow::Result<Response<R>>,
) -> anyhow::Result<()> {
    match result {
        Ok(d) => request.respond(d)?,
        Err(e) => {
            println!("Error encoding {e}");
            request.respond(Response::from_string("error").with_status_code(500))?
        }
    }
    Ok(())
}
