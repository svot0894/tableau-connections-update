import requests
import xml.etree.ElementTree as ET


def sign_in(server, api_version, pat_name, pat_secret, site=""):
    """
    Signs in to the server using a personal access token.

    'server'      specified server address
    'pat_name'    personal access token name
    'pat_secret'  personal access token secret
    'site'        site content URL (default: "")

    Returns the authentication token, site ID, and user ID.
    """
    url = f"{server}/api/{api_version}/auth/signin"
    xmlns = {'t': 'http://tableau.com/api'}

    # Build the request XML
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(
        xml_request,
        'credentials',
        personalAccessTokenName=pat_name,
        personalAccessTokenSecret=pat_secret
    )
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    server_response.raise_for_status()

    # Parse the response
    parsed_response = ET.fromstring(server_response.text)

    # Get the auth token, site ID, and user ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    return token, site_id

def sign_out(server, api_version, auth_token):
    """
    Destroys the active session and invalidates authentication token.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = f"{server}/api/{api_version}/auth/signout"

    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    server_response.raise_for_status()

    return

def get_objects(url, site_id, headers, object_type):
    """
    Fetches all objects of a given type (datasources, flows, workbooks).
    'url'          base server URL
    'site_id'      site ID
    'headers'      request headers including auth token
    'object_type'  type of object to fetch (e.g., "datasources", "flows", "workbooks")
    """
    url = f"{url}/sites/{site_id}/{object_type}"
    xmlns = {'t': 'http://tableau.com/api'}
    params = {'pageSize': 1000}

    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()

    xml_response = ET.fromstring(resp.text)
    objects = xml_response.findall(f".//t:{object_type[:-1]}", namespaces=xmlns)

    return objects

def get_connections(url, site_id, headers, object_type, object_id):
    """
    Fetches all connections for a given object.
    'url'          base server URL
    'site_id'      site ID
    'headers'      request headers including auth token
    'object_type'  type of object (e.g., "datasources", "flows", "workbooks")
    'object_id'    ID of the specific object
    """
    url = f"{url}/sites/{site_id}/{object_type}/{object_id}/connections"
    xmlns = {'t': 'http://tableau.com/api'}

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    xml_response = ET.fromstring(resp.text)
    connections = xml_response.findall(f".//t:connection", namespaces=xmlns)

    return connections

def update_connection(url, site_id, headers, object_type, object_id, connection_id, payload):
    """
    Updates a specific connection for a given object.
    'url'           base server URL
    'site_id'       site ID
    'headers'       request headers including auth token
    'object_type'   type of object (e.g., "datasources", "flows", "workbooks")
    'object_id'     ID of the specific object
    'connection_id' ID of the specific connection to update
    'payload'       dict with connection update details
    """
    url = f"{url}/sites/{site_id}/{object_type}/{object_id}/connections/{connection_id}"
    resp = requests.put(url, headers=headers, json=payload)
    resp.raise_for_status()

    return resp