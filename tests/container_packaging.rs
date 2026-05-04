#[test]
fn dockerfile_defaults_to_service_runtime() {
    let dockerfile = include_str!("../Dockerfile");

    assert!(dockerfile.contains(r#"ENV SPOROS__CONFIG_FILE=/config/config.toml"#));
    assert!(dockerfile.contains(r#"VOLUME ["/config", "/data"]"#));
    assert!(dockerfile.contains("EXPOSE 9000"));
    assert!(dockerfile.contains(r#"ENTRYPOINT ["/app/sporos"]"#));
    assert!(dockerfile.contains(r#"CMD ["serve"]"#));
    assert!(dockerfile.contains("cargo build --release --locked -p sporos --bin sporos"));
    assert!(!dockerfile.contains("ENV CONFIG_DIR=/data"));
    assert!(!dockerfile.contains("EXPOSE 2468"));
}
