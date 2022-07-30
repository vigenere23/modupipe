semantic-release publish --post $@
new_version=$(git tag | tail -1)
git add -u
git commit -m "chore(release): Bumped to $new_version"
git push
