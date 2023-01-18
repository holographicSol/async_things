Face melting file ops.

This bench tries to read 1024 bytes of every file discovered during the pre-scan. And use that 1024 bytes for file type recognition.

Simply adjust proc_max resposibly to find the best time for your specific system.

my system:
proc_max = 16
[pre-scan] time: 1.4287707000039518
[files] 193315
[number of expected chunks] 12083
[multi-process+async] time: 56.86382629998843
