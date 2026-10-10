//! Small OS boundary for private, durable archive snapshots.
use crate::api::{Error, Result};
use std::{fs::File, path::Path};

pub(crate) fn reject_links(path: &Path, regular: bool) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || regular && !meta.is_file() {
                return Err(Error::Io(
                    "archive path must be a regular unlinked file".into(),
                ));
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                if meta.file_attributes() & 0x400 != 0 {
                    return Err(Error::Io("archive reparse points are forbidden".into()));
                }
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if regular && meta.nlink() != 1 {
                    return Err(Error::Io("archive hard links are forbidden".into()));
                }
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
pub(crate) fn verify_parent(path: &Path) -> Result<File> {
    if !path.is_absolute() {
        return Err(Error::InvalidInput("archive path must be absolute".into()));
    }
    for part in path.ancestors() {
        reject_links(part, false)?;
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidInput("archive parent missing".into()))?;
    imp::private_directory(parent)
}
#[cfg(windows)]
fn open_private(path: &Path, create: bool, exclusive: bool) -> Result<File> {
    reject_links(path, true)?;
    let file = imp::open(path, create, exclusive)?;
    imp::check_file(&file)?;
    reject_links(path, true)?;
    Ok(file)
}
#[cfg(windows)]
fn replace(source: &Path, target: &Path) -> Result<()> {
    imp::replace(source, target)
}
/// Once a store is opened, its leaf operations stay relative to the held
/// directory. A rename of that directory cannot move a write to a new inode.
pub(crate) fn open_at(
    directory: &File,
    path: &Path,
    create: bool,
    exclusive: bool,
) -> Result<File> {
    verify_parent_identity(directory, path)?;
    #[cfg(unix)]
    let file = imp::open_at(directory, path, create, exclusive)?;
    #[cfg(windows)]
    let file = open_private(path, create, exclusive)?;
    imp::check_file(&file)?;
    Ok(file)
}
pub(crate) fn replace_at(directory: &File, source: &Path, target: &Path) -> Result<()> {
    verify_parent_identity(directory, target)?;
    #[cfg(unix)]
    {
        imp::replace_at(directory, source, target)
    }
    #[cfg(windows)]
    {
        replace(source, target)
    }
}
pub(crate) fn remove_at(directory: &File, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        imp::remove_at(directory, path)
    }
    #[cfg(windows)]
    {
        let _ = directory;
        std::fs::remove_file(path)?;
        Ok(())
    }
}
pub(crate) fn verify_parent_identity(directory: &File, path: &Path) -> Result<()> {
    let current = verify_parent(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let held = directory.metadata()?;
        let now = current.metadata()?;
        if held.dev() != now.dev() || held.ino() != now.ino() {
            return Err(Error::Io(
                "archive directory identity changed; close the store".into(),
            ));
        }
    }
    #[cfg(windows)]
    {
        let _ = (directory, current);
    }
    Ok(())
}

/// Create one private directory under an existing parent. Existing directories
/// are verified, never silently chmod'ed or assigned new ownership.
pub fn create_private_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::InvalidInput(
            "archive directory must be absolute".into(),
        ));
    }
    for ancestor in path.ancestors() {
        reject_links(ancestor, false)?;
    }
    if !path.exists() {
        imp::create_directory(path)?;
    }
    imp::private_directory(path)?;
    Ok(())
}

/// Exclusively create a migration staging directory under a private parent.
pub(crate) fn create_private_directory_new(path: &Path) -> Result<()> {
    let parent = verify_parent(path)?;
    imp::create_directory(path)?;
    verify_parent_identity(&parent, path)?;
    imp::private_directory(path)?;
    Ok(())
}
/// Publish a complete staged directory without replacing any existing target.
/// The staging directory and all of its handles must be closed first.
pub(crate) fn publish_directory_at(parent: &File, source: &Path, target: &Path) -> Result<()> {
    verify_parent_identity(parent, source)?;
    verify_parent_identity(parent, target)?;
    imp::publish_directory_at(parent, source, target)
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    unsafe extern "C" {
        fn geteuid() -> u32;
        #[link_name = "openat"]
        fn c_openat(dir: i32, path: *const core::ffi::c_char, flags: i32, ...) -> i32;
        #[link_name = "renameat"]
        fn c_renameat(
            fromdir: i32,
            from: *const core::ffi::c_char,
            todir: i32,
            to: *const core::ffi::c_char,
        ) -> i32;
        #[link_name = "unlinkat"]
        fn c_unlinkat(dir: i32, path: *const core::ffi::c_char, flags: i32) -> i32;
    }
    fn uid() -> u32 {
        // SAFETY: geteuid takes no pointers and has no side effects.
        unsafe { geteuid() }
    }
    pub fn private_directory(path: &Path) -> Result<File> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(target_os = "linux")]
        options.custom_flags(0x20000);
        #[cfg(target_os = "macos")]
        options.custom_flags(0x100);
        let f = options.open(path)?;
        let m = f.metadata()?;
        let current = std::fs::symlink_metadata(path)?;
        if !m.is_dir()
            || m.uid() != uid()
            || m.mode() & 0o077 != 0
            || current.file_type().is_symlink()
            || current.dev() != m.dev()
            || current.ino() != m.ino()
        {
            return Err(Error::Io(
                "archive directory must be owned by the current user with mode 0700".into(),
            ));
        }
        Ok(f)
    }
    pub fn create_directory(path: &Path) -> Result<()> {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)?;
        Ok(())
    }
    pub fn check_file(file: &File) -> Result<()> {
        let m = file.metadata()?;
        if !m.is_file() || m.uid() != uid() || m.mode() & 0o077 != 0 || m.nlink() != 1 {
            return Err(Error::Io(
                "archive file must be private and have one link".into(),
            ));
        }
        Ok(())
    }
    fn leaf(path: &Path) -> Result<std::ffi::CString> {
        use std::os::unix::ffi::OsStrExt;
        let name = path
            .file_name()
            .ok_or_else(|| Error::InvalidInput("archive filename required".into()))?;
        std::ffi::CString::new(name.as_bytes())
            .map_err(|_| Error::InvalidInput("archive filename contains NUL".into()))
    }
    pub fn open_at(directory: &File, path: &Path, create: bool, exclusive: bool) -> Result<File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        #[cfg(target_os = "linux")]
        let flags = 2
            | 0x20000
            | 0x80000
            | if create { 0x40 } else { 0 }
            | if exclusive { 0x80 } else { 0 };
        #[cfg(target_os = "macos")]
        let flags = 2
            | 0x100
            | 0x1000000
            | if create { 0x200 } else { 0 }
            | if exclusive { 0x800 } else { 0 };
        let name = leaf(path)?;
        // SAFETY: directory and NUL-terminated single filename are live; mode
        // is passed because O_CREAT may be set. Ownership transfers exactly once.
        let fd = unsafe { c_openat(directory.as_raw_fd(), name.as_ptr(), flags, 0o600u32) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    pub fn replace_at(directory: &File, source: &Path, target: &Path) -> Result<()> {
        use std::os::fd::AsRawFd;
        let from = leaf(source)?;
        let to = leaf(target)?;
        if unsafe {
            c_renameat(
                directory.as_raw_fd(),
                from.as_ptr(),
                directory.as_raw_fd(),
                to.as_ptr(),
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        directory.sync_all()?;
        Ok(())
    }
    pub fn publish_directory_at(parent: &File, source: &Path, target: &Path) -> Result<()> {
        use std::os::fd::AsRawFd;
        #[cfg(target_os = "linux")]
        #[link(name = "dl")]
        unsafe extern "C" {
            fn dlsym(
                handle: *mut core::ffi::c_void,
                name: *const core::ffi::c_char,
            ) -> *mut core::ffi::c_void;
        }
        #[cfg(target_os = "macos")]
        unsafe extern "C" {
            fn renameatx_np(
                fromdir: i32,
                from: *const core::ffi::c_char,
                todir: i32,
                to: *const core::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        let from = leaf(source)?;
        let to = leaf(target)?;
        // SAFETY: live directory descriptor, single NUL-terminated filenames.
        // NOREPLACE/EXCL must be supported; there is no clobbering fallback.
        #[cfg(target_os = "linux")]
        let status = unsafe {
            // Resolve at runtime so this optional migration API does not raise
            // the loader's glibc baseline for existing SDK consumers.
            type Rename = unsafe extern "C" fn(
                i32,
                *const core::ffi::c_char,
                i32,
                *const core::ffi::c_char,
                u32,
            ) -> i32;
            let symbol = dlsym(std::ptr::null_mut(), c"renameat2".as_ptr());
            if symbol.is_null() {
                return Err(Error::Io(
                    "atomic no-replace directory publication unavailable".into(),
                ));
            }
            let rename: Rename = std::mem::transmute(symbol);
            rename(
                parent.as_raw_fd(),
                from.as_ptr(),
                parent.as_raw_fd(),
                to.as_ptr(),
                1,
            )
        };
        #[cfg(target_os = "macos")]
        let status = unsafe {
            renameatx_np(
                parent.as_raw_fd(),
                from.as_ptr(),
                parent.as_raw_fd(),
                to.as_ptr(),
                4,
            )
        };
        if status != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        parent.sync_all()?;
        Ok(())
    }
    pub fn remove_at(directory: &File, path: &Path) -> Result<()> {
        use std::os::fd::AsRawFd;
        let name = leaf(path)?;
        if unsafe { c_unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use std::{
        os::windows::{
            ffi::OsStrExt,
            io::{AsRawHandle, FromRawHandle},
        },
        ptr,
    };
    use windows_sys::Win32::{
        Foundation::{CloseHandle, INVALID_HANDLE_VALUE, LocalFree},
        Security::{
            ACCESS_ALLOWED_ACE, ACL,
            Authorization::{
                ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
                GetSecurityInfo, SE_FILE_OBJECT,
            },
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetTokenInformation,
            OWNER_SECURITY_INFORMATION, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL,
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
            FILE_SHARE_WRITE, GetFileInformationByHandle, MOVEFILE_REPLACE_EXISTING,
            MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_ALWAYS, OPEN_EXISTING,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };
    // WinNT.h ACE_HEADER::AceType values. Keep this boundary independent of
    // the broad Win32_System_SystemServices feature used only for constants.
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    const ACCESS_DENIED_ACE_TYPE: u8 = 1;
    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }
    struct Local(*mut core::ffi::c_void);
    impl Drop for Local {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
    }
    fn failure() -> Error {
        std::io::Error::last_os_error().into()
    }
    fn private_sddl(directory: bool) -> Result<Vec<u16>> {
        // Set the owner explicitly: an elevated token's default owner can be
        // the Administrators group, which is not this host user's identity.
        let token = current_sid()?;
        unsafe {
            let user = ptr::read_unaligned(token.as_ptr().cast::<TOKEN_USER>())
                .User
                .Sid;
            let mut text = ptr::null_mut();
            if ConvertSidToStringSidW(user, &mut text) == 0 {
                return Err(failure());
            }
            let _text = Local(text.cast());
            let mut len = 0;
            while *text.add(len) != 0 {
                len += 1;
            }
            let sid = String::from_utf16(std::slice::from_raw_parts(text, len))
                .map_err(|_| Error::Io("invalid current-user SID".into()))?;
            Ok(format!(
                "O:{sid}D:P(A;{};FA;;;OW)",
                if directory { "OICI" } else { "" }
            )
            .encode_utf16()
            .chain(Some(0))
            .collect())
        }
    }
    pub fn open(path: &Path, create: bool, exclusive: bool) -> Result<File> {
        let mut sd = ptr::null_mut();
        let sddl = private_sddl(false)?;
        // SAFETY: SDDL and output pointers remain valid throughout these calls.
        unsafe {
            if ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                1,
                &mut sd,
                ptr::null_mut(),
            ) == 0
            {
                return Err(failure());
            }
            let _sd = Local(sd);
            let sa = SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: sd,
                bInheritHandle: 0,
            };
            let handle = CreateFileW(
                wide(path).as_ptr(),
                0x80000000 | 0x40000000,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &sa,
                if exclusive {
                    CREATE_NEW
                } else if create {
                    OPEN_ALWAYS
                } else {
                    OPEN_EXISTING
                },
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                ptr::null_mut(),
            );
            if handle == INVALID_HANDLE_VALUE {
                return Err(failure());
            }
            Ok(File::from_raw_handle(handle))
        }
    }
    fn current_sid() -> Result<Vec<u8>> {
        unsafe {
            let mut token = ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return Err(failure());
            }
            let mut length = 0;
            GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut length);
            let mut bytes = vec![0u8; length as usize];
            let ok = GetTokenInformation(
                token,
                TokenUser,
                bytes.as_mut_ptr().cast(),
                length,
                &mut length,
            );
            CloseHandle(token);
            if ok == 0 {
                return Err(failure());
            }
            Ok(bytes)
        }
    }
    fn check_owner_acl(file: &File) -> Result<()> {
        unsafe {
            let mut owner = ptr::null_mut();
            let mut dacl: *mut ACL = ptr::null_mut();
            let mut sd = ptr::null_mut();
            let status = GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut owner,
                ptr::null_mut(),
                &mut dacl,
                ptr::null_mut(),
                &mut sd,
            );
            if status != 0 {
                return Err(std::io::Error::from_raw_os_error(status as i32).into());
            }
            let _sd = Local(sd);
            let token = current_sid()?;
            let user = ptr::read_unaligned(token.as_ptr().cast::<TOKEN_USER>())
                .User
                .Sid;
            if owner.is_null() || dacl.is_null() || EqualSid(owner, user) == 0 {
                return Err(Error::Io("archive owner or private ACL mismatch".into()));
            }
            for index in 0..(*dacl).AceCount as u32 {
                let mut raw = ptr::null_mut();
                if GetAce(dacl, index, &mut raw) == 0 {
                    return Err(failure());
                }
                let ace = &*(raw.cast::<ACCESS_ALLOWED_ACE>());
                if ace.Header.AceType == ACCESS_DENIED_ACE_TYPE {
                    continue;
                }
                if ace.Header.AceType != ACCESS_ALLOWED_ACE_TYPE {
                    return Err(Error::Io("unsupported archive ACL entry".into()));
                }
                let sid = ptr::addr_of!(ace.SidStart).cast_mut().cast();
                // OWNER RIGHTS (S-1-3-4) is permitted for a current-user-owned file.
                let owner_rights: [u8; 12] = [1, 1, 0, 0, 0, 0, 0, 3, 4, 0, 0, 0];
                if EqualSid(sid, user) == 0
                    && EqualSid(sid, owner_rights.as_ptr().cast_mut().cast()) == 0
                {
                    return Err(Error::Io(
                        "archive ACL grants another principal access".into(),
                    ));
                }
            }
            Ok(())
        }
    }
    pub fn check_file(file: &File) -> Result<()> {
        unsafe {
            let mut info: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
            if GetFileInformationByHandle(file.as_raw_handle(), &mut info) == 0 {
                return Err(failure());
            }
            if info.nNumberOfLinks != 1 || info.dwFileAttributes & 0x400 != 0 {
                return Err(Error::Io("archive linked file rejected".into()));
            }
        }
        check_owner_acl(file)
    }
    pub fn private_directory(path: &Path) -> Result<File> {
        unsafe {
            let h = CreateFileW(
                wide(path).as_ptr(),
                0x80000000,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                ptr::null_mut(),
            );
            if h == INVALID_HANDLE_VALUE {
                return Err(failure());
            }
            let file = File::from_raw_handle(h);
            let mut info: BY_HANDLE_FILE_INFORMATION = std::mem::zeroed();
            if GetFileInformationByHandle(file.as_raw_handle(), &mut info) == 0 {
                return Err(failure());
            }
            if info.dwFileAttributes & 0x10 == 0 || info.dwFileAttributes & 0x400 != 0 {
                return Err(Error::Io(
                    "archive directory handle is a reparse point or non-directory".into(),
                ));
            }
            check_owner_acl(&file)?;
            Ok(file)
        }
    }
    pub fn create_directory(path: &Path) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;
        let sddl = private_sddl(true)?;
        unsafe {
            let mut sd = ptr::null_mut();
            if ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                1,
                &mut sd,
                ptr::null_mut(),
            ) == 0
            {
                return Err(failure());
            }
            let _sd = Local(sd);
            let sa = SECURITY_ATTRIBUTES {
                nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: sd,
                bInheritHandle: 0,
            };
            if CreateDirectoryW(wide(path).as_ptr(), &sa) == 0 {
                return Err(failure());
            }
        }
        Ok(())
    }
    pub fn publish_directory_at(_parent: &File, source: &Path, target: &Path) -> Result<()> {
        // No MOVEFILE_REPLACE_EXISTING: even an empty target is never replaced.
        unsafe {
            if MoveFileExW(
                wide(source).as_ptr(),
                wide(target).as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            ) == 0
            {
                return Err(failure());
            }
        }
        Ok(())
    }
    pub fn replace(source: &Path, target: &Path) -> Result<()> {
        unsafe {
            if MoveFileExW(
                wide(source).as_ptr(),
                wide(target).as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            ) == 0
            {
                return Err(failure());
            }
        }
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
compile_error!("The durable file archive currently requires Windows, Linux, or macOS.");
