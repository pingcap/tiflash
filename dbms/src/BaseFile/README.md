# BaseFile

BaseFile is a file system abstraction layer that provides a common interface for file system operations. All file system operations should be performed through BaseFile.

**Note**

> This directory should not include any files in other directories except for `Common`.

- WritableFile: A writable file abstraction. It provides all the functions that a file system should support for writing.
- RandomAccessFile: A random access file abstraction. It provides all the functions that a file system should support for random access.
- WriteReadableFile: A writable and readable file abstraction. It provides all the functions that a file system should support for both writing and reading.
- PosixXxxFile: A file abstraction for posix file system.
- RateLimiter: Used to control read/write rate.
