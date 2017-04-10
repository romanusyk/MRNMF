/**
 * Created by romm on 03.04.17.
 */
public class CLIOption {

    private String name;
    private boolean hasArg;
    private String description;

    private boolean required;

    public CLIOption(String name, boolean hasArg, String description) {
        this.name = name;
        this.hasArg = hasArg;
        this.description = description;
        required = true;
    }

    public CLIOption(String name, boolean hasArg, String description, boolean required) {
        this.name = name;
        this.hasArg = hasArg;
        this.description = description;
        this.required = required;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean hasArg() {
        return hasArg;
    }

    public void setHasArg(boolean hasArg) {
        this.hasArg = hasArg;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    @Override
    public String toString() {
        return "\t-" + name + (hasArg ? " <arg> " : " ") + ": " + description;
    }

}
