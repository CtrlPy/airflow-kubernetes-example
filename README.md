# airflow-kubernetes-example


1.step:

```zsh

helm repo add apache-airflow https://airflow.apache.org
helm repo update

```

add values 

install helm:
```zsh
helm install airflow apache-airflow/airflow -f values.yaml
```
upgrade chart:
```zsh

helm upgrade airflow apache-airflow/airflow -f values.yaml -n airflow


```
Fernet Key: V1RsMnM2bkFqdTl0OE1wY3U1dUdXcHVNa0ozRzJkZmw=

