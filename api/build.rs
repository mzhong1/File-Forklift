use std::process::Command;

fn main() {
    // call flatc
    let output = Command::new("/usr/local/bin/flatc")
        .args(&[
            "-r",
            "-o",
            "src/",
            "protos/service.fbs",
        ]).output()
        .expect("Failed to execute process");
    println!("status: {}", output.status);
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success());
}
