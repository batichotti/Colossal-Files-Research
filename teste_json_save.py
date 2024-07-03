from pydriller import Repository
import json
import time


commits_data = []
i = 1
start_time = time.time()
for commit in Repository('https://github.com/refinedmods/refinedstorage2').traverse_commits():
    commit_dict = {
        'hash': commit.hash,
        'author': commit.author.name,
        'author_email': commit.author.email,
        'author_date': str(commit.author_date),
        'committer': commit.committer.name,
        'committer_email': commit.committer.email,
        'committer_date': str(commit.committer_date),
        'msg': commit.msg,
        'modifications': [
            {
                'old_path': mod.old_path,
                'new_path': mod.new_path,
                'change_type': str(mod.change_type),
                'added_lines': mod.added_lines,
                'deleted_lines': mod.deleted_lines,
                'nloc': mod.nloc,
                'complexity': mod.complexity
            } for mod in commit.modified_files
        ],
        'project_path': str(commit.project_path),
        'deletions': commit.deletions,
        'insertions': commit.insertions
    }
    with open(f'commit_{i}.json', 'w', encoding='utf-8') as json_file:
        json.dump(commit_dict, json_file, ensure_ascii=False, indent=4)
    i += 1
end_time = time.time()
print(f'Execution time: \033[35m{(end_time-start_time)/60} minutes for {i} commits\033[m')
with open("commit_1.json", "r", encoding='utf-8') as json_file:
    commit_data = json.load(json_file)
    print(commit_data['author_email'])
