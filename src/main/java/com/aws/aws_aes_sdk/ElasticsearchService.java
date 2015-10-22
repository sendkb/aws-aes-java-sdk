package com.aws.aws_aes_sdk;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.HttpMethodName;


public class ElasticsearchService {
    private AWSCredentials credentials;
    private String endpoint;
    private String region;

    public ElasticsearchService(AWSCredentials creds, String endpoint, String region) {
        this.credentials = creds;
        this.region = region;
        this.endpoint = endpoint;
    }
    
    public void performRequest(String resourcePath, String strParams, Map<String,String[]>params, HttpMethodName method, PrintWriter out) throws IOException {
        String requestUri = endpoint + resourcePath;

        URL query = new URL(requestUri);
        HttpURLConnection conn = (HttpURLConnection) query.openConnection();
        Map<String, String> mapParams = new HashMap<String, String>();
        Iterator<String> keys = params.keySet().iterator();
        
        while (keys.hasNext()) {
            String key = keys.next();
            mapParams.put(key, params.get(key)[0]);
            strParams += key + "=" + URLEncoder.encode(params.get(key)[0],"UTF-8") + "&";
        }

        Request<?> awsRequest = QuerySigner.createRequest(strParams, mapParams, QuerySigner.SDK_SERVICE_NAME, endpoint, resourcePath, method);
        QuerySigner.addSignatureV4Headers(conn, awsRequest, this.credentials, this.region);

        conn.setRequestProperty("User-Agent", "AWS Elasticsearch Service Java Client");
        conn.setRequestProperty("Accept", "application/json, text/javascript, */*");
        conn.setRequestMethod(method.name());
        
        if (method == HttpMethodName.POST) {
            conn.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
            wr.writeBytes(strParams);
            wr.flush();
            wr.close();
        } 
        
        conn.connect();

        int responseCode = conn.getResponseCode();
        if ( responseCode == 200 ) {
            readAndSpew(conn.getInputStream(), out);
        } else {
            readAndSpew(conn.getErrorStream(), out); 
        }
    }

    private void readAndSpew( InputStream stream, PrintWriter out ) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(stream,"UTF-8")); 
        String inputLine;

        while ((inputLine = in.readLine()) != null) {
            out.println(inputLine);
            System.out.print(inputLine);
        }

        in.close();
    }
}