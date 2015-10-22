package com.aws.aws_aes_sdk;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.http.HttpMethodName;

public class QuerySignerTest {
    public static void main(String[] args) throws IOException {
        String protocol = "https://";
        String endpoint = protocol + "<DOMAIN_ENDPOINT";
        HttpMethodName method = HttpMethodName.GET;
        
        String accessKey = "<ACCESS_KEY>";
        String secretKey = "<SECRET_KEY>";
        String region = "<REGION>";
        
        String strParams = "";
        Map<String,String[]>mapParams = new HashMap<String, String[]>();
        //TODO: use DefaultAWSCredentialsProviderChain
        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);


        //GET Request
        String resourcePath = "/_cat/nodes";
        ElasticsearchService aes = new ElasticsearchService(creds, endpoint, region);
        aes.performRequest(resourcePath, strParams, mapParams, method, new PrintWriter(System.out));

        //PUT request
        resourcePath = "/twitter";
        method = HttpMethodName.PUT;
        aes.performRequest(resourcePath, strParams, mapParams, method, new PrintWriter(System.out));
        
        resourcePath = "/_cat/indices";
        method = HttpMethodName.GET;
        aes.performRequest(resourcePath, strParams, mapParams, method, new PrintWriter(System.out));
        
        //TODO: test for POST and DELETE
    }

}
