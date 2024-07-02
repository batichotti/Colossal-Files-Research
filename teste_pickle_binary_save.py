from pydriller import Repository
import pickle

commits_data = []
i = 1
for commit in Repository('https://github.com/refinedmods/refinedstorage2').traverse_commits():
    with open(f'commit_{i}', 'ab') as arquivo_binario:
      pickle.dump(commit, arquivo_binario)
    i += 1


with open("commit_1", "rb") as arquivo_binario:
  while True:
    try:
        obj = pickle.load(arquivo_binario)
        print(obj.hash)
    except EOFError:
        break
