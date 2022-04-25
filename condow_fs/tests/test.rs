use condow_fs::{Condow, FsClient};

fn create_condow_condow() -> Condow<FsClient> {
    FsClient::condow(Default::default()).unwrap()
}

fn get_test_file_path() -> String {
    format!(
        "{}/tests/test_data",
        std::env::current_dir().unwrap().display()
    )
}

#[tokio::test]
async fn download_full() {
    let condow = create_condow_condow();

    let data = condow
        .blob()
        .location(get_test_file_path())
        .range(..)
        .download()
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
        .blob()
        .location(get_test_file_path())
        .range(..5)
        .download()
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
        .blob()
        .location(get_test_file_path())
        .range(..=26)
        .download()
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
        .blob()
        .location(get_test_file_path())
        .range(10..)
        .download()
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
        .blob()
        .location(get_test_file_path())
        .range(1..11)
        .download()
        .await
        .unwrap()
        .into_vec()
        .await
        .unwrap();

    assert_eq!(&data[..], b"bcdefghijk");
}
