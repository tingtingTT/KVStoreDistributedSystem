#!/usr/bin/python
import re
import os
import sys

###
# Instructions for running this test script

# Purpose: This is to ensure that your repository contains all the necessary files for us to automatically pull and build

# Instructions to run
# 1. Copy this file into a folder of its own
# 2. Make sure it has permissions to run
# 3. Specify as input the http link of the commit ID that you will be submitting, like so:
#     ./run_fall17.py <HTtp_link_to_commit_id>
# 4. You should see a message that indicates that the docker file is built successfully.
# Debug any issues you see until you get a sucess message

# Other Instructions
# Ensure that your repo is shared with either ashakarim or KamalaRamas
###

pattern_repo = re.compile("bitbucket\.org\/.*\/commits\/")
pattern_commit = re.compile("\/commits\/[a-z0-9]+")
pattern_white_space = re.compile(r'\s+')

def log(main_dir, msg):
    filename = main_dir + "/log.txt"
    if 'log.txt' in os.listdir(main_dir):
        log_file = open(filename, 'a')
    else:
        log_file = open(filename, 'w')
    log_file.write(str(msg) + "\n")
    log_file.close()

def parse_commit_link(http_str):
    matches = pattern_repo.findall(http_str)
    if (matches) == 0:
        raise Exception("Incorrect format for bitbucket commit string: " + hpttp_str)
    repo = matches[0][len('bitbucket.org/'):-len("/commits/")]
    matches = pattern_commit.findall(http_str)
    if (matches) == 0:
        raise Exception("Incorrect format for bitbucket commit string: " + http_str)
    commit = matches[0][len('/commits/') :]
    return repo, commit

def git_clone(http_str):
    repo, commit = parse_commit_link(http_str)
    path = './' + commit
    if os.path.isdir(path):
        print "directory " + path + " already exists. Skip pulling repository."
        return commit
    os.mkdir(path)
    print "created directory " + path
    os.chdir(path)
    print "cd to " + path
    print "git clone ..."
    os.system('git clone git@bitbucket.org:' + repo + '.git')
    os.chdir('..')
    return commit

def has_file(dir_path, filename):
    return filename in os.listdir(dir_path)

def list_subdir(dir_path):
    return [name for name in os.listdir(dir_path) if os.path.isdir(dir_path + "/" + name)]

def find_file(dir_path, filename):
    queue = [dir_path]
    target_paths = []
    while len(queue) > 0:
        path = queue.pop()
        if has_file(path, filename):
            target_paths.append(path)
        for subdir in list_subdir(path):
            queue.insert(0, path + "/" + subdir)
    if len(target_paths) == 1:
        return target_paths[0]
    return None

def compose_container_name(filename):
    cname = None
    try:
        infile = open(filename, 'r')
        for l in infile:
            l.rstrip("\n")
            l = re.sub(pattern_white_space, '', l)
            if len(l) > 0:
                if  cname is None:
                    cname = l
                else:
                    cname += "/" + l
    except:
        raise Exception("Problems with processing file: " + filename)
    return cname

def run(http_str, homeworknum):
    suffix = "_hw" + str(homeworknum)
    main_dir = os.getcwd()
    repo, commit = parse_commit_link(http_str)
    git_clone(http_str)
    os.chdir("./" + commit)
    #
    members_dir = find_file(".", "members.txt")
    if members_dir is None:
        log(main_dir, "Cannot find members.txt in " + commit)
        os.chdir(main_dir)
        return
    container_name = compose_container_name(members_dir + "/members.txt")
    container_name += suffix
    #
    docker_dir = find_file(".", "Dockerfile")
    if docker_dir is None:
        log(main_dir, "Cannot find Dockerfile in " + commit)
        os.chdir(main_dir)
        return
    os.chdir(docker_dir)
    os.system('git reset --hard ' + commit)
    print "running:  + docker build -t " + container_name + " ."
    status = os.system("docker build -t " + container_name + " .")
    if status == 0:
        log(main_dir, "Successfully built container " + container_name + " for commit " + commit)
    else:
        log(main_dir, "Failed to build container " + container_name + "for commit " + commit + " status " + str(status))
    os.chdir(main_dir)

if __name__ == "__main__":
    http_str = sys.argv[1]
#"https://bitbucket.org/cmps128testing/proj1/commits/cbc9c2cd57a9086101396dc553114d86c7db0075"
    run(http_str, 1)
