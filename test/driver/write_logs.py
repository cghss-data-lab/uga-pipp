def write_log(file_name: str, message: str) -> None:
    file_path = f"./test/logs/{file_name}.log"
    with open(file_path, "a") as log_file:
        msg = f"ERROR:\t{message}\n"
        log_file.write(msg)
