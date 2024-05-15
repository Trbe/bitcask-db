use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse, Redirect},
    routing::get,
    Form, Router,
};
use bitcask::{Handle, KeyValueStorage};
use bytes::Bytes;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;

const PATH: &str = "./database";

#[tokio::main]
async fn main() -> Result<()> {
    let db = bitcask::Bitcask::open(PATH)?;

    let app = Router::new()
        .route("/", get(index).post(post_url))
        .route("/s/:id", get(get_url))
        .with_state(db.get_handle());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(
        r###"
    <form action="/" method="post">
        <input name="url">
        <button type="submit">submit</button>
    </form>
    "###,
    )
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct Input {
    url: Bytes,
}

async fn post_url(State(db): State<Handle>, Form(input): Form<Input>) -> String{
    let rng = rand::thread_rng();
    let id = rng.sample_iter(&Alphanumeric).take(8).collect::<Bytes>();
    match db.set(id.clone(), input.url) {
        Ok(_) => unsafe { "http://192.168.122.1:3000/s/".to_string() + std::str::from_utf8_unchecked(&id) },
        Err(_) => String::from("Error"),
    }
}

async fn get_url(Path(id): Path<Bytes>, State(db): State<Handle>) -> impl IntoResponse{
    match db.get(id).unwrap() {
        Some(url) => Redirect::permanent(unsafe {
            std::str::from_utf8_unchecked(&url)
        }),
        None => Redirect::permanent("/"),
    }
}