import requests
from decouple import config

# GitHub Access token
GITHUB_TOKEN = config("GITHUB_TOKEN")

def get_file_from_github(organization, repository, branch, path_to_file, personal_access_token=GITHUB_TOKEN):
    url = f'https://raw.githubusercontent.com/{organization}/{repository}/{branch}/{path_to_file}'
    headers = {'Authorization': f'token {personal_access_token}'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.content.decode('utf-8')
    else:
        print(f'Failed to retrieve file: {response.status_code}')
        return None


# def add_comment_to_issue(owner, repo, issue_number, comment, authorization_token):
#     url = f'https://api.github.com/repos/{owner}/{repo}/issues/{issue_number}/comments'
#     headers = {
#         'Authorization': f'Bearer {authorization_token}',
#         "Accept": "application/vnd.github+json"
#     }
#     data = {'body': comment}
#     response = requests.post(
#         url,
#         headers=headers,
#         json=data
#     )
#
#     if response.status_code == 201:
#         print("Successfully added comment to issue.")
#     else:
#         print(f"Failed to add comment to issue. Status code: {response.status_code}")
#         print(response.json())
