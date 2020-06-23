package com.redhat.demo.dm.ccfraud;

import com.google.gson.Gson;
import com.redhat.demo.dm.ccfraud.domain.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class CaseMgmt {

    public void invokeCase(PotentialFraudFact potentialFraudFact) {
        try {
            String kieServerUrl = System.getProperty("kie.server.url", "cat-pam-kieserver:8080");
            String kieContainer = System.getProperty("kie.container.id", "test-case-project_1.0.0");
            String processDefinitionId = System.getProperty("process.definition.id", "src.fraudWorkflow");
            String authBase64Encoded = System.getProperty("kie.server.auth.token", "src.fraudWorkflow");

            URL url = new URL(kieServerUrl +
                    "/services/rest/server/containers/" + kieContainer + 
                    "/processes/" + processDefinitionId +
                    "/instances");
            
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Authorization","Basic " + authBase64Encoded);

            PotentialFraudFactCaseFile potentialFraudFactCaseFile = 
             new PotentialFraudFactCaseFile(
                 String.valueOf(potentialFraudFact.getCreditCardNumber()),potentialFraudFact.getTransactions().toString());
            String transactionList = "";

            for(CreditCardTransaction transaction:potentialFraudFact.getTransactions()) {
                if(transactionList.trim().isEmpty()) {
                transactionList = transaction.getTransactionNumber() + "";
                } else {
                    transactionList = "," + transaction.getTransactionNumber();
                }
            }

            potentialFraudFactCaseFile.setCaseFile_transactionList(transactionList);

            OutputStream os = conn.getOutputStream();
            os.write(new Gson().toJson(potentialFraudFactCaseFile).getBytes());
            os.flush();

            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            conn.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
