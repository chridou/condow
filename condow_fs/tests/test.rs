use condow_fs::{Condow, FsClient};

fn create_condow_condow() -> Condow<FsClient> {
    FsClient::condow(Default::default()).unwrap()
}

fn get_test_file_path() -> url::Url {
    let mut path = std::env::current_dir().unwrap();
    path.push("tests");
    path.push("test_data");
    url::Url::from_file_path(path).expect("path should be absolute")
}

#[tokio::test]
async fn download_full() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), ..)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"abcdefghijklmnopqrstuvwxyz");
}

#[tokio::test]
async fn download_to() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), ..5)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"abcde");
}

#[tokio::test]
async fn download_to_end() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), ..=26)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"abcdefghijklmnopqrstuvwxyz");
}

#[tokio::test]
async fn download_from() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), 10..)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"klmnopqrstuvwxyz");
}

#[tokio::test]
async fn download_from_to() {
    let condow = create_condow_condow();

    let data = condow
        .download(get_test_file_path(), 1..11)
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"bcdefghijk");
}
