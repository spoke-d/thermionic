package fsys

func DeleteVirtualFileSystemFiles(fs *VirtualFileSystem, path string) {
	delete(fs.files, path)
}
