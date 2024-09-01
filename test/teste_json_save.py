from pydriller import Repository
import json
import time
import os
import re

def sanitize_string(s):
    return re.sub(r'[^\x00-\x7F]+', '', s)

commits_data = []
i = 1
start_time = time.time()
repository_path = 'https://github.com/AlDanial/cloc'
output_dir = 'peak'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for commit in Repository(repository_path).traverse_commits():
    commit_dict = {
        'hash': commit.hash,
        'author': commit.author.name,
        'author_email': commit.author.email,
        'author_date': str(commit.author_date),
        'committer': commit.committer.name,
        'committer_email': commit.committer.email,
        'committer_date': str(commit.committer_date),
        'msg': sanitize_string(commit.msg),
        '# files': commit.files,
        'project_path': str(commit.project_path),
        'deletions': commit.deletions,
        'insertions': commit.insertions
    }

    modifications = []
    for j, mod in enumerate(commit.modified_files, start=1):
        mod_dict = {
            'commit_hash': commit.hash,
            'old_path': mod.old_path,
            'new_path': mod.new_path,
            'change_type': str(mod.change_type),
            'added_lines': mod.added_lines,
            'deleted_lines': mod.deleted_lines,
            'nloc': mod.nloc,
            'complexity': mod.complexity
        }
        mod_file_path = os.path.join(output_dir, f'commit_{i}_file_{j}.json')
        with open(mod_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(mod_dict, json_file, ensure_ascii=False, indent=4)
        modifications.append(mod_file_path)

    commit_dict['modifications_files'] = modifications

    commit_file_path = os.path.join(output_dir, f'commit_{i}.json')
    with open(commit_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(commit_dict, json_file, ensure_ascii=False, indent=4)

    i += 1

end_time = time.time()
print(f'Execution time: \033[35m{(end_time-start_time)/60} minutes for {i-1} commits\033[m')

with open(os.path.join(output_dir, "commit_1.json"), "r", encoding='utf-8') as json_file:
    commit_data = json.load(json_file)
    print(commit_data['author_email'])
