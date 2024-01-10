package tp6;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Club {
    @Id
    private long id;

    private int version;

    private String fabricant;
    private Double poids;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFabricant() {
        return fabricant;
    }

    public void setFabricant(String fabricant) {
        this.fabricant = fabricant;
    }

    public Double getPoids() {
        return poids;
    }

    public void setPoids(Double poids) {
        this.poids = poids;
    }

}
