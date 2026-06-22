package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = new RestTemplate();
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
        );

        float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        transactionRecordRepository.save(
                new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount)
        );

        UserRecord wilbur = userRepository.findById(9);
        if (wilbur != null) {
            System.out.println("Wilbur balance: " + wilbur.getBalance());
        }
    }
}