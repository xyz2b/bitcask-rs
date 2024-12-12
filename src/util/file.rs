use std::path::PathBuf;

/// 获取磁盘剩余空间
pub fn available_disk_size(dir_path: PathBuf) -> u64 {
    if let Ok(size) = fs2::free_space(dir_path) {
        return size;
    }
    0
}

/// 磁盘数据目录的大小
pub fn dir_disk_size(dir_path: PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_disk_size() {
        let path = PathBuf::from("/tmp/test");
        let free_space = available_disk_size(path);
        println!("{:?}", free_space / 1024 / 1024 / 1024);
    }
}
