import pandas as pd

languages = [
    "C",
    "C#",
    "C++",
    "Dart",
    "Go",
    "Java",
    "JavaScript",
    "Kotlin",
    "Objective-C",
    "PHP",
    "Python",
    "Ruby",
    "Rust",
    "Swift",
    "TypeScript",
]

input_path = "./src/_15/output/per_language/"

concat = pd.DataFrame()

for language in languages:
    repo = pd.read_csv(f"{input_path}{language}.csv")
    repo.insert(0, 'Language', language)
    concat = pd.concat([concat, repo], ignore_index=True)

concat.to_csv("./src/_15/output/result.csv", index=False)
