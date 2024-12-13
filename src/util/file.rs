use std::{fs, io, path::PathBuf};

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

/// 拷贝数据目录
pub fn copy_dir(src: PathBuf, dest: PathBuf, exculde: &[&str]) -> io::Result<()> {
    if !dest.exists() {
        fs::create_dir_all(&dest)?;
    }

    for dir_entry in fs::read_dir(src)? {
        let entry = dir_entry?;
        let src_path = entry.path();

        if exculde.iter().any(|&x| src_path.ends_with(x)) {
            continue;
        }

        println!("{:?}", entry.file_name());

        let dest_path = dest.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(src_path, dest_path, exculde)?;
        } else {
            fs::copy(src_path, dest_path)?;
        }
    }

    Ok(())
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
