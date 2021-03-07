package ru.gex.mdm_proxy.elk_consumer.domain;

public class Constant {
    public static final String NAMESPACE_URI = "http://xxxxxxxxxxxxxxxxxxxxxxxxx";

    public static final int NOT_SUPPORT_CODE = 14;
    public static final String NOT_SUPPORT_DESCRIPTION = "Not supported";

    public static final int OK_CODE = 0;
    public static final String OK_DESCRIPTION = "OK";

    //Внутренняя ошибка сервиса
    public static final int SERVICE_ERROR_CODE = -1;
    public static final String SERVICE_ERROR_DESCRIPTION = "Error";

    //Сервис временно недоступен
    public static final int SERVICE_NOT_ACCESSABLE_CODE = -2;
    public static final String SERVICE_NOT_ACCESSABLE_DESCRIPTION = "Temporary not accessable";



}
