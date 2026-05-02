package tn.utm.kafka.models;

public class EventPOS {
    public String type;
    public String idCaisse;
    public String ville;
    public String timestamp;
    public double montant;
    public String[] produits;

    public EventPOS() {}
}