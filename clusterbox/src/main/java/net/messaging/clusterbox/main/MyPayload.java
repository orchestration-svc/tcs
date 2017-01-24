package net.messaging.clusterbox.main;

public class MyPayload {
    private String firstName;
    private Integer age;
    private String lastName;

    public MyPayload() {

    }

    public MyPayload(String firstName, Integer age, String lastName) {
        super();
        this.firstName = firstName;
        this.age = age;
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "MyPayload [firstName=" + firstName + ", age=" + age + ", lastName=" + lastName + "]";
    }
}
