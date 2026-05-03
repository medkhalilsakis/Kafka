package tn.utm.kafka.models;

import java.util.Arrays;

public class EventPOS {
    private String type;
    private String idCaisse;
    private String ville;
    private String timestamp;
    private double montant;
    private String[] produits;

    public EventPOS() {
    }

    public EventPOS(String type, String idCaisse, String ville, String timestamp, double montant, String[] produits) {
        this.type = type;
        this.idCaisse = idCaisse;
        this.ville = ville;
        this.timestamp = timestamp;
        this.montant = montant;
        this.produits = produits;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIdCaisse() {
        return idCaisse;
    }

    public void setIdCaisse(String idCaisse) {
        this.idCaisse = idCaisse;
    }

    public String getVille() {
        return ville;
    }

    public void setVille(String ville) {
        this.ville = ville;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public double getMontant() {
        return montant;
    }

    public void setMontant(double montant) {
        this.montant = montant;
    }

    public String[] getProduits() {
        return produits;
    }

    public void setProduits(String[] produits) {
        this.produits = produits;
    }

    @Override
    public String toString() {
        return "EventPOS{" +
                "type='" + type + '\'' +
                ", idCaisse='" + idCaisse + '\'' +
                ", ville='" + ville + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", montant=" + montant +
                ", produits=" + Arrays.toString(produits) +
                '}';
    }
}
