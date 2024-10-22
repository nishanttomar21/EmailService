// Logic for sending emails - https://www.digitalocean.com/community/tutorials/javamail-example-send-mail-in-java-smtp
// How to install kafka on Mac - https://learn.conduktor.io/kafka/how-to-install-apache-kafka-on-mac/
// @KafkaListener - Defines a method that will be invoked when a message is received from a Kafka topic.
// Properties - Contains configuration properties for the Kafka consumer.
// Command to start Zookeeper (Go inside the kafka folder and then use the command) - bin/zookeeper-server-start.sh config/zookeeper.properties
// Command to start Kafka (Go inside the kafka folder and then use the command) - bin/kafka-server-start.sh config/server.properties
// Set App Password in Gmail app and use that password here in password - https://myaccount.google.com/apppasswords
// export GMAIL_PASSWORD="password"

package org.example.emailserice.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.emailserice.utils.EmailUtil;
import org.example.emailserice.dtos.EmailDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;

@Component
public class SendEmailConsumer {

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics="user_signedup", groupId = "emailService")    // groupId: unique identifier for the consumer group, so that only 1 email is sent to user. Only 1 instance of Email Service instance will process the message and not all instances.
    public void sendEmail(String message) {
        try {
            EmailDto emailDto = objectMapper.readValue(message, EmailDto.class);    // converts JSON string to a Java object


            Properties props = new Properties();
            props.put("mail.smtp.host", "smtp.gmail.com"); //SMTP Host
            props.put("mail.smtp.port", "587"); //TLS Port
            props.put("mail.smtp.auth", "true"); //enable authentication
            props.put("mail.smtp.starttls.enable", "true"); //enable STARTTLS

            //create Authenticator object to pass in Session.getInstance argument
            Authenticator auth = new Authenticator() {
                //override the getPasswordAuthentication method
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(emailDto.getFrom(), System.getenv("GMAIL_PASSWORD"));  // (fromEmail, password)
                }
            };

            Session session = Session.getInstance(props, auth);

            EmailUtil.sendEmail(session, emailDto.getTo(), emailDto.getSubject(), emailDto.getBody());

        }catch (JsonProcessingException exception) {
            throw new RuntimeException(exception.getMessage());
        }
    }
}