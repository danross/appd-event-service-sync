.\Scripts\activate.ps1
pip freeze > requirements.txt
$comment = Read-Host -Prompt 'Enter GIT comment'
git commit -a -m "$comment"
git push