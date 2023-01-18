Face melting file ops.

This bench discovers files during the pre-scan. And then tries to read 1024 bytes of each file into magic for file recognition.

Simply adjust proc_max resposibly to find the best time for your specific system.

my system (MSI GL62 6QF Laptop. i7-6700HQ CPU 16GB RAM):
proc_max = 16
[pre-scan] time: 1.4287707000039518 seconds
[files] 193315
[number of expected chunks] 12083
[multi-process+async] time: 56.86382629998843 seconds
