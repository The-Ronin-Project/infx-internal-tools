import httpx


def get_token(url: str, client_id: str, client_secret: str, audience: str) -> str:
    """
    Fetches a token from Auth0.
    """
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": "client_credentials",
    }
    response = httpx.post(url, json=payload)
    token = response.json()["access_token"]
    return token


# Function to use the token to access the API
def make_get_request(token, client: httpx.Client, base_url, api_url, params={}):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


async def make_get_request_async(
    token, client: httpx.AsyncClient, base_url, api_url, params={}
):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.get(base_url + api_url, headers=headers, params=params)
    return response.json()


async def make_post_request_async(
    token, client: httpx.AsyncClient, base_url, api_url, params={}
):
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
    response = await client.post(base_url + api_url, headers=headers, params=params)
    return response.json()
