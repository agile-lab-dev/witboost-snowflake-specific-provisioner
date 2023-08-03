<!-- Generator: Widdershins v4.0.1 -->

<h1 id="specific-provisioner-micro-service">Snowflake Specific Provisioner API</h1>

Scroll down for code samples, example requests and responses.

<h1 id="specific-provisioner-micro-service-specificprovisioner">SpecificProvisioner</h1>

All the provisioning related operations

## provision

<a id="opIdprovision"></a>

### Code samples

<details>
  <summary>Shell</summary>

```shell
# You can also use wget
curl -X POST /datamesh.mwaaspecificprovisioner/{{version}}/provision \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```
</details>

<details>
  <summary>JavaScript</summary>

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/datamesh.mwaaspecificprovisioner/{{version}}/provision',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```
</details>

<details>
  <summary>Java</summary>

```javaURL obj = new URL("/datamesh.mwaaspecificprovisioner/{{version}}/provision");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```
</details>

<details>
  <summary>Python</summary>

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/datamesh.mwaaspecificprovisioner/{{version}}/provision', headers = headers)

print(r.json())

```
</details>

`POST /provision`

*Deploy a data product starting from its descriptor*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}
```

<h3 id="provision-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|A provisioning request descriptor wrapped as a string into a simple object|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string"
}
```

<h3 id="provision-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the request result|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|If successful returns a provisioning deployment task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[ValidationError](#schemavalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## getStatus

<a id="opIdgetStatus"></a>

### Code samples

<details>
  <summary>Shell</summary>

```shell
# You can also use wget
curl -X GET /datamesh.mwaaspecificprovisioner/{{version}}/provision/{token}/status \
  -H 'Accept: application/json'

```
</details>

<details>
  <summary>JavaScript</summary>

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('/datamesh.mwaaspecificprovisioner/{{version}}/provision/{token}/status',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```
</details>

<details>
  <summary>Java</summary>

```java
URL obj = new URL("/datamesh.mwaaspecificprovisioner/{{version}}/provision/{token}/status");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());
```
</details>

<details>
  <summary>Python</summary>

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('/datamesh.mwaaspecificprovisioner/{{version}}/provision/{token}/status', headers = headers)

print(r.json())

```
</details>

`GET /provision/{token}/status`

*Get the status for a provisioning request*

<h3 id="getstatus-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|token|path|string|true|token that identifies the request|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string"
}
```

<h3 id="getstatus-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|The request status|[ProvisioningStatus](#schemaprovisioningstatus)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[ValidationError](#schemavalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## validate

<a id="opIdvalidate"></a>

### Code samples

<details>
  <summary>Shell</summary>

```shell
# You can also use wget
curl -X POST /datamesh.mwaaspecificprovisioner/{{version}}/validate \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```
</details>

<details>
  <summary>JavaScript</summary>

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/datamesh.mwaaspecificprovisioner/{{version}}/validate',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```
</details>

<details>
  <summary>Java</summary>

```java
URL obj = new URL("/datamesh.mwaaspecificprovisioner/{{version}}/validate");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());
```
</details>

<details>
  <summary>Python</summary>

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/datamesh.mwaaspecificprovisioner/{{version}}/validate', headers = headers)

print(r.json())

```
</details>

`POST /validate`

*Validate a provisioning request*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}
```

<h3 id="validate-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|A provisioning request descriptor wrapped as a string into a simple object|

> Example responses

> 200 Response

```json
{
  "valid": true,
  "error": {
    "errors": [
      "string"
    ]
  }
}
```

<h3 id="validate-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns a specific reply containing the validation result|[ValidationResult](#schemavalidationresult)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## unprovision

<a id="opIdunprovision"></a>

### Code samples

```shell
# You can also use wget
curl -X POST /datamesh.mwaaspecificprovisioner/{{version}}/unprovision \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```
</details>

<details>
  <summary>JavaScript</summary>

```javascript
const inputBody = '{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/datamesh.mwaaspecificprovisioner/{{version}}/unprovision',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```
</details>

<details>
  <summary>Java</summary>

```javaURL obj = new URL("/datamesh.mwaaspecificprovisioner/{{version}}/unprovision");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```
</details>

<details>
  <summary>Python</summary>

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/datamesh.mwaaspecificprovisioner/{{version}}/unprovision', headers = headers)

print(r.json())

```
</details>

`POST /unprovision`

*Undeploy a data product starting from its descriptor*

> Body parameter

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}
```

<h3 id="unprovision-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[ProvisioningRequest](#schemaprovisioningrequest)|true|A provisioning request descriptor wrapped as a string into a simple object|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string"
}
```

<h3 id="unprovision-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the request result|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|If successful returns a provisioning deployment task token that can be used for polling the request status|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[ValidationError](#schemavalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

## updateacl

<a id="opIdupdateacl"></a>

### Code samples

<details>
  <summary>Shell</summary>

```shell
# You can also use wget
curl -X POST /datamesh.mwaaspecificprovisioner/{{version}}/updateacl \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```
</details>

<details>
  <summary>JavaScript</summary>

```javascript
const inputBody = '{
  "acl": {
    "users": [
      "string"
    ],
    "groups": [
      "string"
    ]
  },
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('/datamesh.mwaaspecificprovisioner/{{version}}/updateacl',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```
</details>

<details>
  <summary>Java</summary>

```java
URL obj = new URL("/datamesh.mwaaspecificprovisioner/{{version}}/updateacl");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());
```
</details>

<details>
  <summary>Python</summary>

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('/datamesh.mwaaspecificprovisioner/{{version}}/updateacl', headers = headers)

print(r.json())

```
</details>

`POST /updateacl`

*Request the access to a specific provisioner component*

> Body parameter

```json
{
  "acl": {
    "users": [
      "string"
    ],
    "groups": [
      "string"
    ]
  },
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}
```

<h3 id="updateacl-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[UpdateAclRequest](#schemaupdateaclrequest)|true|An access request object|

> Example responses

> 200 Response

```json
{
  "status": "RUNNING",
  "result": "string"
}
```

<h3 id="updateacl-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|It synchronously returns the access request response|[ProvisioningStatus](#schemaprovisioningstatus)|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|It synchronously returns the access request response|string|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid input|[ValidationError](#schemavalidationerror)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|System problem|[SystemError](#schemasystemerror)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_UpdateAclRequest">UpdateAclRequest</h2>
<!-- backwards compatibility -->
<a id="schemaupdateaclrequest"></a>
<a id="schema_UpdateAclRequest"></a>
<a id="tocSupdateaclrequest"></a>
<a id="tocsupdateaclrequest"></a>

```json
{
  "acl": {
    "users": [
      "string"
    ],
    "groups": [
      "string"
    ]
  },
  "provisionInfo": {
    "request": "string",
    "result": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|acl|[Acl](#schemaacl)|true|none|none|
|provisionInfo|[ProvisionInfo](#schemaprovisioninfo)|true|none|none|

<h2 id="tocS_DescriptorKind">DescriptorKind</h2>
<!-- backwards compatibility -->
<a id="schemadescriptorkind"></a>
<a id="schema_DescriptorKind"></a>
<a id="tocSdescriptorkind"></a>
<a id="tocsdescriptorkind"></a>

```json
"DATAPRODUCT_DESCRIPTOR"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|DATAPRODUCT_DESCRIPTOR|
|*anonymous*|COMPONENT_DESCRIPTOR|
|*anonymous*|DATAPRODUCT_DESCRIPTOR_WITH_RESULTS|

<h2 id="tocS_ProvisioningRequest">ProvisioningRequest</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioningrequest"></a>
<a id="schema_ProvisioningRequest"></a>
<a id="tocSprovisioningrequest"></a>
<a id="tocsprovisioningrequest"></a>

```json
{
  "descriptorKind": "DATAPRODUCT_DESCRIPTOR",
  "descriptor": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|descriptorKind|[DescriptorKind](#schemadescriptorkind)|true|none|none|
|descriptor|string|true|none|A provisioning request in yaml format|

<h2 id="tocS_ProvisioningStatus">ProvisioningStatus</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioningstatus"></a>
<a id="schema_ProvisioningStatus"></a>
<a id="tocSprovisioningstatus"></a>
<a id="tocsprovisioningstatus"></a>

```json
{
  "status": "RUNNING",
  "result": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|none|
|result|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|RUNNING|
|status|COMPLETED|
|status|FAILED|

<h2 id="tocS_ValidationResult">ValidationResult</h2>
<!-- backwards compatibility -->
<a id="schemavalidationresult"></a>
<a id="schema_ValidationResult"></a>
<a id="tocSvalidationresult"></a>
<a id="tocsvalidationresult"></a>

```json
{
  "valid": true,
  "error": {
    "errors": [
      "string"
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|valid|boolean|true|none|none|
|error|[ValidationError](#schemavalidationerror)|false|none|none|

<h2 id="tocS_ValidationError">ValidationError</h2>
<!-- backwards compatibility -->
<a id="schemavalidationerror"></a>
<a id="schema_ValidationError"></a>
<a id="tocSvalidationerror"></a>
<a id="tocsvalidationerror"></a>

```json
{
  "errors": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errors|[string]|true|none|none|

<h2 id="tocS_Acl">Acl</h2>
<!-- backwards compatibility -->
<a id="schemaacl"></a>
<a id="schema_Acl"></a>
<a id="tocSacl"></a>
<a id="tocsacl"></a>

```json
{
  "users": [
    "string"
  ],
  "groups": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|users|[string]|true|none|none|
|groups|[string]|true|none|none|

<h2 id="tocS_ProvisionInfo">ProvisionInfo</h2>
<!-- backwards compatibility -->
<a id="schemaprovisioninfo"></a>
<a id="schema_ProvisionInfo"></a>
<a id="tocSprovisioninfo"></a>
<a id="tocsprovisioninfo"></a>

```json
{
  "request": "string",
  "result": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|request|string|true|none|none|
|result|string|false|none|none|

<h2 id="tocS_SystemError">SystemError</h2>
<!-- backwards compatibility -->
<a id="schemasystemerror"></a>
<a id="schema_SystemError"></a>
<a id="tocSsystemerror"></a>
<a id="tocssystemerror"></a>

```json
{
  "error": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|string|true|none|none|

