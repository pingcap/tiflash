#!/usr/bin/python
import os
import subprocess
import sys

copied = {}
done = {}

def pathOf(path):
    return path.split('/')[-1]

def copyLib(path):
    #print("copy lib %s" % path)
    basePath = pathOf(path)
    if basePath in copied:
        return
    #print("copy: file %s to current directory" % basePath)
    try:
        subprocess.check_call(["cp", path, "."])
    except:
        print("copy %s to current directory failed" % path)
        sys.exit(1)
    try:
        subprocess.check_call(["chmod", "+w", basePath])
    except:
        print("chmod +x %s failed" % basePath)
        sys.exit(1)
    copied[path] = True

def changeToLocal(path):
    #print("change to local %s" % path)
    basePath = pathOf(path)
    if path in done:
        return []
    output = subprocess.Popen(["/usr/bin/otool", "-L", path], stdout=subprocess.PIPE).communicate()[0]
    res = []
    for i, line in enumerate(output.split('\n')):
        line = line.strip()
        if line.startswith("/usr/local/opt"):
            lib = line.split()[0]
            lib = lib[:-1] if lib[-1] = ':' else lib
            #print("add lib %s" % lib)
            res.append(lib)
    for lib in res:
        libPath = pathOf(lib)
        #print("changeToLocal: changing path of %s in %s," % (libPath, basePath))
        if libPath == basePath:
            subprocess.Popen(["install_name_tool", "-id", "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]
        else:
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]
    for i, line in enumerate(output.split('\n')):
        line = line.strip()
        if line.startswith("@rpath"):
            lib = line.split()[0]
            libPath = pathOf(lib)
            subprocess.Popen(["install_name_tool", "-change", lib, "@executable_path/%s" % libPath, basePath], stdout=subprocess.PIPE).communicate()[0]
    done[path] = True
    return res

def run(path):
    #copyLib(path)
    libPath = pathOf(path)
    libs = changeToLocal(path)
    copyPath = path
    while len(libs) > 0:
        path = libs.pop()
        libPath = pathOf(path)
        copyLib(path)
        newLibs = changeToLocal(path)
        for lib in newLibs:
            if not lib in done and not lib in libs:
                libs.insert(0, lib)

if __name__ == '__main__':
    run(sys.argv[1])
