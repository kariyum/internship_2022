cd C:\Users\Intern\Documents\karim-theIntern\dimension-hierarchy-dep
kubectl delete -f ./k8s
docker rmi karimtheintern/client:v0.2
docker build --tag karimtheintern/client:v0.2 ./frontier
docker push karimtheintern/client:v0.2
docker build --tag karimtheintern/server:v0.2 ./spark-learning1
docker push karimtheintern/server:v0.2
kubectl apply -f ./k8s
