from pydriller import Repository
import pickle

commits_data = []
i = 1
for commit in Repository('https://github.com/iptv-org/iptv').traverse_commits():
    with open(f'commit_{i}', 'ab') as arquivo_binario:
      pickle.dump(commit, arquivo_binario)
    i += 1
