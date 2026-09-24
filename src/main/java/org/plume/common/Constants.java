package org.plume.common;

public class Constants {

    public static final String ACKS = "all";
    private static final String DEAD_LETTER_QUEUE_SUFFIX = "_dlq";
    private static final String ERROR_SUFFIX = "_err";

    public static String getOrInferDlqTopic(String dlqTopic, String originalTopic) {
        return dlqTopic != null
            ? dlqTopic : originalTopic + DEAD_LETTER_QUEUE_SUFFIX;
    }

    public static String getOrInferErrorTopic(String errorTopic, String originalTopic) {
        return errorTopic != null
            ? errorTopic : originalTopic + ERROR_SUFFIX;
    }
}
