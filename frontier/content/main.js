
const canvas = document.getElementById("screen");
const ctx = canvas.getContext("2d");

// document.getElementById("get").addEventListener("click", event =>{
//     const xhttp = new XMLHttpRequest();
//     xhttp.onload = function () {
//         // document.getElementById("response").innerHTML = this.responseText
//         console.log(this.responseText)
//     }
//     xhttp.open("GET", "http://localhost:5050/dimension_check")
//     xhttp.send();
//     console.log('Request sent')
// })


// document.getElementById("test-request").addEventListener("click", e =>{
//     const xhttp = new XMLHttpRequest();
//     xhttp.onload = function () {
//         document.getElementById("response").innerHTML = this.responseText
//     }
//     xhttp.open("POST", "http://localhost:5050/hello");
//     xhttp.send();
//     console.log('Request sent');
// })

window.onload = (event) => {
    localStorage.clear();
};