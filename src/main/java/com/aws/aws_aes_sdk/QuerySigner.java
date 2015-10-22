package com.aws.aws_aes_sdk;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;

public class QuerySigner {

    public static final String SDK_SERVICE_NAME = "es";
    
    /**
     * Created AWS request to be signed
     * This generic method will use request method to call eigher setParameters or setContent.
     * This method is recomended for calling RESTful APIs
     */
    public static Request createRequest(String strParams, Map<String, String> params, String serviceName, String endpoint, String resourcePath, HttpMethodName methodName) {
        Request<?> request = new DefaultRequest<Void>(serviceName);
        request.setHttpMethod(methodName);
        request.setEndpoint(URI.create(endpoint));
        request.setResourcePath(resourcePath);

        if (methodName == HttpMethodName.POST || methodName == HttpMethodName.PUT) {
            InputStream stream = new ByteArrayInputStream(strParams.getBytes());
            request.setContent(stream);
        } else if (methodName == HttpMethodName.GET || methodName == HttpMethodName.DELETE) {
            params = new TreeMap<String, String>(params);
            request.setParameters(params);
        }

        return request;
    }

    public static Request createPostRequest(String strParams, String serviceName, String endpoint, String resourcePath) {
        return createRequest(strParams, null, serviceName, endpoint, resourcePath, HttpMethodName.POST);
    }

    public static Request createGetRequest(Map<String, String> params, String serviceName, String endpoint, String resourcePath) {
        return createRequest(null, params, serviceName, endpoint, resourcePath, HttpMethodName.GET);
    }

    public static void addSignatureV4Headers(HttpURLConnection conn, Request<?> request, AWSCredentials credentials, String regionName) {
        // 3. Sign the request
        AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setServiceName(request.getServiceName());
        aws4Signer.setRegionName(regionName);
        aws4Signer.sign(request, credentials);

        // 4. Update connection headers
        for (Map.Entry<String, String> header : request.getHeaders().entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }
    }

    /**
     * Creates AWS signature version 4 request headers and add them to connection.
     * Here use the default service name.
     */
    public static void addSignatureV4Headers(HttpURLConnection conn, AWSCredentials credentials, 
    HttpMethodName methodName, String endpoint, String resourcePath, Map<String, String> params, 
    String strParams, String regionName) 
    throws UnsupportedEncodingException {
        addSignatureV4Headers(conn, credentials, methodName, endpoint, resourcePath, params, strParams, regionName, SDK_SERVICE_NAME);
    }

    /**
     * Creates AWS signature version 4 request headers and add them to connection.
     * TODO: optimize this method for 1) GET/POST 2) use auth bundle
     */
    public static void addSignatureV4Headers(HttpURLConnection conn, AWSCredentials credentials,
    HttpMethodName methodName, String endpoint, String resourcePath, Map<String, String> params,
    String strParams, String regionName, String serviceName)
    throws UnsupportedEncodingException {

        Request<?> request = null;
        if (methodName == HttpMethodName.POST) {
            request = createPostRequest(strParams, serviceName, endpoint, resourcePath);
        } else {
            request = createGetRequest(params, serviceName, endpoint, resourcePath);
        }
        addSignatureV4Headers(conn, request, credentials, regionName);

    }
}
