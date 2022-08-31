// import { Network } from "./classes/network.js";

const scale = 0.5
const url_path = "/api"

function  getMousePos(canvas, evt) {
    var rect = canvas.getBoundingClientRect(), // abs. size of element
        scaleX = canvas.width / rect.width,    // relationship bitmap vs. element for x
        scaleY = canvas.height / rect.height;  // relationship bitmap vs. element for y

    return {
        x: (evt.clientX - rect.left) * scaleX,   // scale mouse coordinates after they have
        y: (evt.clientY - rect.top) * scaleY     // been adjusted to be relative to element
    }
}

document.getElementById("add").addEventListener("click", addNode)




function addNode(){
    const childElement = document.getElementById("child-val")
    const parentElement = document.getElementById("parent-val")
    if (childElement.value == "") childElement.value = "child"
    if (parentElement.value == "") parentElement.value = "parent"
    network.addEdge(childElement.value, parentElement.value)
    console.log("place nodes start")
    // network.placeNodes()
    network.positionNodes()
    // network.pushDownNodes()
}

function clearScreen(){
    ctx.fillStyle = 'rgba(255, 255, 255, 1)';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
}
const network = new Network()

window.requestAnimationFrame(frame)

function frame(){
    clearScreen()
    network.update()
    window.requestAnimationFrame(frame)
}

document.getElementById("map-id").addEventListener("click", (event)=>{
    console.log( Object.fromEntries(network.map))
})

function getData(){
    if (typeof network != "undefined") return JSON.stringify(Object.fromEntries(network.map));
    else return null
}

document.getElementById("dimension_check").addEventListener("click", event =>{
    const xhttp = new XMLHttpRequest();
    xhttp.onload = function () {
        document.getElementById("response").innerHTML = this.responseText
    }
    xhttp.open("POST", url_path+"/dimension_check")
    xhttp.setRequestHeader("Content-Type", "application/json;charset=UTF-8");
    const data = getData();
    if (data.length > 2){
        xhttp.send(data);
        console.log('Request sent', data)
    }
    else{
        console.log('Request prohibited, data is undefined.')
    }
})

document.getElementById("hierarchy_check").addEventListener("click", event =>{
    const xhttp = new XMLHttpRequest();
    xhttp.onload = function () {
        document.getElementById("response").innerHTML = this.responseText
    }
    xhttp.open("POST", url_path+"/hierarchy_check")
    xhttp.setRequestHeader("Content-Type", "application/json;charset=UTF-8");
    const data = getData();
    if (data.length > 2){
        xhttp.send(data);
        console.log('Request sent', data)
    }
    else{
        console.log('Request prohibited, data is undefined.')
    }
})

function appendTable(obj){

    const data = obj["payload"] // array[JSON-OBJ]
    const array_order = ["tenant_id", "dimension_id", "JSON Object","validity", "drawing"]

    const tr = document.createElement("tr")
    for(const key of array_order){
        const th = document.createElement("th")
        th.innerHTML = key
        tr.appendChild(th)
    }
    document.getElementById("dimension-table").appendChild(tr)

    for(const entry of data){ // entry is a json object 
        const tr = document.createElement("tr")
        for (const att of array_order){
            const td = document.createElement("td")
            if (att == "drawing" && entry["validity"]==true){
                const canvas = document.createElement("canvas")
                const n = new Network(canvas)
                const map = entry["mymap"]
                for( let key in map ){
                    // aux_map.set(key, js_object[key])
                    for(let key2 in map[key]){
                        n.addEdge(key, map[key][key2])
                    }
                }
                n.positionNodes()
                n.update()
                td.appendChild(canvas)
                tr.appendChild(td)
            }else{
                if (att == "JSON Object") {
                    const pre = document.createElement("pre")
                    
                    var str = JSON.stringify(entry["mymap"], undefined, 2);
                    console.log(str)
                    // code.innerHTML = str
                    // pre.appendChild(code)
                    pre.innerHTML = str
                    td.appendChild(pre)
                    tr.appendChild(td)
                }
                else{
                    td.innerHTML = entry[att]
                    tr.appendChild(td)
                }
            }
        }
        document.getElementById("dimension-table").appendChild(tr)
    }
    
}

document.getElementById("submit-dimension-config-file").addEventListener("click", event=>{
    document.getElementById("dimension-spinner").hidden = false
    const xhttp = new XMLHttpRequest();
    var formData = new FormData();
    if (document.getElementById("file-input").files[0] == undefined){
        alert("Please select a dimension configuration file then proceed.");
        return;
    }
    formData.append("file", document.getElementById("file-input").files[0]);
    
    xhttp.onload = function () {
        // document.getElementById("response").innerHTML = this.responseText
        const json_response = JSON.parse(this.responseText)
        // const js_object = json_response["payload"][3]["mymap"]
        // for( let key in js_object ){
        //     // aux_map.set(key, js_object[key])
        //     for(let key2 in js_object[key]){
        //         network.addEdge(key, js_object[key][key2])
        //     }
        // }
        // network.positionNodes()
        // network.update()
        // console.log(json_response)
        appendTable(json_response)
        console.log(json_response["temp_path"][0])
        localStorage.setItem("temp_path", json_response["temp_path"][0])
        document.getElementById("dimension-spinner").hidden = true
    }
    xhttp.open("POST", url_path+"/dimension_check_csv")
    xhttp.send(formData);

})

const div_elements = []

// print -- chunked data
document.getElementById("submit-hierarchy-config-file").addEventListener("click", event => {
    const xhttp = new XMLHttpRequest();
    document.getElementById("hierarchy-spinner").hidden = false
    var formData = new FormData();
    if ("file", document.getElementById("hierarchy-file-input").files[0] == undefined){
        alert("Upload hierarchies file.")
        return;
    }
    formData.append("file", document.getElementById("hierarchy-file-input").files[0]);
    if (localStorage.getItem("temp_path") === null){
        alert("Please validate the dimensions first.");
        return;
    }
    formData.append("temp_path", localStorage.getItem("temp_path"))
    xhttp.onload = function (){
        const json_response = JSON.parse(this.responseText)
        console.log(json_response)
        document.getElementById("data-tables").innerHTML = ""
        for(const obj of json_response["payload"]){
            
            const div = document.createElement("div")
            div.setAttribute("class", "data-group")
            
            const canvas = document.createElement("canvas")
            const n = new Network(canvas)
            const map = obj[0]["path"]
            console.log("MAPPPP", map)
            for( let key in map ){
                n.addEdge(key, map[key])
            }
            n.positionNodes()
            n.update()
            // document.getElementById("data-tables").append(canvas)
            const table = appendDataTable(obj) // Input -> array of json objects
            
            div.appendChild(canvas)
            div.appendChild(table)
            document.getElementById("data-tables").appendChild(div)
            div_elements.push(div)
            document.getElementById("hierarchy-spinner").hidden = true
        }
        document.getElementById("next-btn").hidden = false
        document.getElementById("prev-btn").hidden = false

    }

    xhttp.open("POST", url_path+"/generate_data_hierarchy_print")
    xhttp.send(formData)
})

function appendDataTable(json_arr){
    const order_arr = ["tenant_id", "hierarchy_id", "hierarchy_name", "dimension_id", "child_level", "parent_level", "parent_element_id", "child_element_id", "child_element_label", "parent_element_label"]
    const table = document.createElement("table")
    const first_row = document.createElement("tr")
    for(const col of order_arr){
        const th = document.createElement("th")
        th.innerHTML = col
        first_row.appendChild(th)
    }
    table.appendChild(first_row)
    
    for(const obj of json_arr){
        // obj is a json object containing ["tenant_id", "hierarchy_id", "hierarchy_name", "dimension_id", "child_level", "parent_level", "parent_element_id", "child_element_id", "child_element_label", "parent_element_label"]
        const tr = document.createElement("tr")
        for(const col of order_arr){
            const td = document.createElement("td")
            td.innerHTML = obj[col]
            tr.appendChild(td)
        }
        table.appendChild(tr)
    }

    return table
}

document.getElementById("download-csv").addEventListener("click", event => {
    document.getElementById("hierarchy-spinner").hidden = false
    const xhttp = new XMLHttpRequest();
    var formData = new FormData();
    formData.append("file", document.getElementById("hierarchy-file-input").files[0]);
    if (document.getElementById("hierarchy-file-input").files[0]==undefined){
        alert("Upload hierarchies file.")
        return
    }
    if (localStorage.getItem("temp_path") === null){
        alert("Please validate the dimensions first.")
        return
    }
    formData.append("temp_path", localStorage.getItem("temp_path"))
    xhttp.onload = function (){
        console.log(this.responseURL)
        console.log(this.responseText)
        saveData(this.responseText, "hierarchy_data.csv");
        document.getElementById("hierarchy-spinner").hidden = true
    }

    xhttp.open("POST", url_path+"/generate_data_hierarchy_csv")
    xhttp.send(formData)
})

var saveData = (function () {
    var a = document.createElement("a");
    document.body.appendChild(a);
    a.style = "display: none";
    return function (data, fileName) {
        var json = data,
            blob = new Blob([json], {type: "octet/stream"}),
            url = window.URL.createObjectURL(blob);
        a.href = url;
        a.download = fileName;
        a.click();
        window.URL.revokeObjectURL(url);
    };
}());


var index = 0;
document.getElementById("next-btn").addEventListener("click", () => {
    if (index < div_elements.length){
        if (index != div_elements.length-1){
            index+=1;
        }
        div_elements[index].scrollIntoView({behavior: "smooth"});
    }
})

document.getElementById("prev-btn").addEventListener("click", ()=>{
    if (index >= 0){
        if (index != 0){
            index -=1;
        }
        div_elements[index].scrollIntoView({behavior: "smooth"});
    }
})
