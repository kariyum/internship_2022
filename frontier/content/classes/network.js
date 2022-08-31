
class Vector2{
    constructor(x, y){
        this.x = x;
        this.y = y;
    }
}
const SCALE = 1
const DRAWCIRCLE = 0
class NodeStructure {
    constructor(x, y){
        this.pos = new Vector2(x, y)
        this.r = 50 * SCALE
        this.acc = 0
    }
    setValue(str){
        this.value = str;
    }
    updatePos(x, y){
        this.pos = new Vector2(x, y)
    }
    addForce(a){
        this.displacement+= a;
    }
    applyForce(){
        this.x += this.displacement;
    }
    draw(ctx){
        if (DRAWCIRCLE){
            ctx.strokeStyle = 'rgba(0, 0, 0, 1)'
            ctx.beginPath();
            ctx.arc(this.pos.x, this.pos.y, this.r, 0, 2 * Math.PI);
            ctx.stroke();
        }
        ctx.textAlign = 'left';
        ctx.font = `${SCALE*24}px sans-serif`;
        ctx.fillStyle = 'rgba(0, 0, 0, 1)';
        ctx.fillText(this.value, this.pos.x - this.value.length*3, this.pos.y);
    }
}
class Network{
    constructor(canvasElement = document.getElementById("screen")){
        this.nodes = new Array();
        this.map = new Map(); // map[String, Array[String]]

        this.scale = 0.3;
        this.canvas = canvasElement
        this.ctx = this.canvas.getContext('2d')
        this.canvas.width = window.innerWidth * this.scale
        this.canvas.height = window.innerWidth * this.scale
        
    }
    addNode(value){
        const x = this.canvas.getBoundingClientRect().width/2
        const y = this.canvas.getBoundingClientRect().height/2
        const n = new NodeStructure(x, y)
        n.setValue(value)
        this.nodes.push(n)
    }
    addEdge(child, parent){
        // check if child is already created
        if (this.existsMap(child)){
            // check if parent is in the values
            if (! this.map.get(child).includes(parent)){
                this.map.get(child).push(parent)
            }
        }
        else{
            this.map.set(child, new Array(parent))
        }
        if (!this.existsNodes(parent)){
            this.addNode(parent)
        }
        if (!this.existsNodes(child)){
            this.addNode(child)
        }
        //verified.

        // this.map.forEach((v, k)=>{
        //     document.body.append(`${k}->${v}`)
        // })
    }

    existsMap(targetVal){
        for(const [key, value] of this.map){
            if (key == targetVal) return true            
        }
        return false
    }

    existsNodes(targetNode){
        for(const node of this.nodes){
            if (node.value == targetNode ) return true
        }
        return false
    }
    draw(){
        this.nodes.forEach(node => {
            node.draw(this.ctx);
        });
    }
    update(){
        this.drawEdges();
        this.draw();
    }
    placeNodes(){ // distribute the nodes into levels
        // make an array of levels
        // place the nodes in the array to their corresponding level
        // find out the coordinates based on how dense the level is 
        const levels = new Map();
        var submap = new Map(this.map);
        console.log(this.map.size)
        var levelIdx = 0;
        while (true) {
            // deletes empty entries key -> nothing
            const emptyEntries = this.getEmptyEntries(submap);
            this.popEmptyEntries(submap, emptyEntries)
            
            submap = this.popValues(submap, emptyEntries);
            const v = this.getValues(submap)
            const k = this.getKeys(submap)
            // console.log("Values", v)
            // console.log("Keys", k)
            
            console.log(emptyEntries)
            const topop = this.toPop(k, v)
            // console.log("intersect", topop)

            submap = this.popValues(submap, topop);
            if (topop.concat(emptyEntries).length != 0) levels.set(levelIdx++, topop.concat(emptyEntries))
            console.log(submap.size, topop.length)
            if (submap.size == 0) {
                console.log("Dimension verified.")
                break;
            }
            if (topop.concat(emptyEntries).length == 0){
                console.log("Cycle detected.")
                // remove last added edge
                const new_map = new Map();
                var key_count = 0;
                for (const [key, value] of this.map){
                    new_map.set(key, value)
                    key_count++;
                    if (key_count == this.map.size - 1){
                        this.map = new_map;
                        break;
                    }
                }
                break;
            }
            
            // console.log("submap", submap)
        }
        console.log("levels", levels)
        // console.log(this.canNode("parent", 1, levels))
        // this.placeNodeInLevel("parent", 1, levels);
        return levels; // map of {'levelid' : Array(String)}
        // levels.forEach((k,v)=>document.body.append(`${k}->${v}`))
    }
    pushDownNodes(){
        // for each node in the levels map we gonna try to push them down, if there aren't any nodes from the next_level poiting to the current_node
        // we keep the change
        var levels = this.placeNodes();
        
        // retrieve all nodes and try to put them in the farthest level possible
        for(let i=levels.size-1; i>=0; i--){
            const nodes_arr = levels.get(i); // nodes of level i
            // iterate over the nodes and try to place them in the farthest level which is 0 and up until i
            for (const node of nodes_arr){
                console.log(`current node ${node}`);
                // get max level
                // place at that max level
                const max_level = this.getMaxLevel(node, levels);
                console.log("Max level", max_level)
                levels = this.placeNodeInLevel(node, max_level, levels)
            }
        }
        return levels;
    }
    
    getMaxLevel(node_value, levels){
        const pred_arr = new Array();
        // find out the nodes that point sto node_value
        for (const [key, value] of this.map){
            if (value.includes(node_value)){
                pred_arr.push(key)
            }
        }

        // get their levels.
        const level_arr = new Array();
        for( const node of pred_arr ){
            for( const [lvl_index, sub_lvl_arr] of levels){
                if (sub_lvl_arr.includes(node)){
                    level_arr.push(lvl_index);
                }
            }
        }

        // get current level
        var node_value_level = 0;
        for (const [lvlidx, level] of levels){
            if (level.includes(node_value)){
                node_value_level = lvlidx;
            }
        }
        console.log(level_arr);
        console.log(node_value, Math.max(...level_arr)-1);
        if (level_arr.length == 0) return node_value_level;
        return (Math.min(...level_arr)-1);
    }
    canBePlaced(nodeValue, levelIdx, sublevels){
        // check if the node in that level isn't pointed by any other node in the levelIdx
        // for every node key check if nodeValue is in the values
        console.log("can node", sublevels)
        const nodesInLvl = sublevels.get(levelIdx);
        for(const node of nodesInLvl){
            // check if node points to nodeValue
            console.log(node, nodeValue)
            console.log(this.map.get(node))
            if (this.map.get(node).includes(nodeValue)) return false
        }
        return true
    }
    placeNodeInLevel(node_value, levelIdx, sublevels){
        // get node_value level 
        // remove it from it's current level
        // add it to the destination level
        var node_value_level = 0;
        for (const [lvlidx, level] of sublevels){
            if (level.includes(node_value)){
                node_value_level = lvlidx;
            }
        }
        console.log(`Placing ${node_value} from ${node_value_level} to ${levelIdx}`);
        if (node_value_level == levelIdx) return sublevels;
        sublevels.get(levelIdx).push(node_value);
        const arr = Array()
        for (const str of sublevels.get(node_value_level)){
            if (str != node_value) arr.push(str);
        }
        sublevels.set(node_value_level, arr);
        console.log("End placeNodeInLevel", sublevels);
        return sublevels;
    }
    positionNodes(left=0, top=0, width=0, height=0){ // attach coordinates to the nodes
        // get the number of levels 
        // get the radius*scale of each node
        // divide canvas height into levels => that's the y's 
        var levels = this.pushDownNodes();
        levels = this.seperateNodes(levels);
        const nbLvl = levels.size;
        const top_margin = 50;
        const bottom_margin = 20;
        const right_margin = 0;
        const left_margin = 0;

        const y_step = (this.canvas.height- (top_margin + bottom_margin)) / nbLvl
        
        for(const [key, lvl] of levels){
            const x_step = (this.canvas.width ) / lvl.length
            const y = y_step * key
            var x_multiplier = 0;
            for(const nodeVal of lvl){
                if (nodeVal == "") continue;
                const target_node = this.getNodeFromVal(nodeVal);
                target_node.pos.y = top_margin + y;
                target_node.pos.x = x_step * x_multiplier + this.canvas.width/2 - (x_step * (lvl.length-1) /2);
                x_multiplier++;
            }
        }
        
        console.log(this.getNodeFromVal("parent"))

        // get the number of nodes per level
        // divide the width into the number of nodes per level => that's the x's

        // draw nodes.
        return levels;
    }
    seperateNodes(sublevels){
        console.log(sublevels)
        for (const [key_node, succ_arr] of this.map){
            const node_value = key_node
            var source_level = 0;
            for (const [lvlidx, level] of sublevels){
                console.log("level.inclues", level)
                if (level.includes(node_value)){
                    source_level = lvlidx;
                }
            }

            for(const next_node of succ_arr){
                var next_level = 0;
                const node_value = next_node
                for (const [lvlidx, level] of sublevels){
                    if (level.includes(node_value)){
                        next_level = lvlidx;
                    }
                }
                if (source_level - next_level >=2 && sublevels.get(next_level+1).length == 1) {
                    const arr = new Array();
                    for (const e of sublevels.get(next_level+1) ){
                        arr.push(e);
                    }
                    arr.push("")
                    
                    sublevels.set(next_level+1, arr)
                }
            }

        }
        return sublevels;

    }
    getValues(subMap){
        // returns an array of all values of the map
        const values = new Array();
        for(const v of subMap.values()){
            for(const v2 of v){
                if (!values.includes(v2)){
                    values.push(v2)
                }
            }
        }
        return values
    }
    getKeys(subMap){
        // returns an array of keys of the map
        const keys = new Array()
        for (const k of subMap.keys()){
            keys.push(k)
        }
        return keys
    }
    toPop(keys, values){ // nodes that points to nothing
        return values.filter( k => ! keys.includes(k))
    }
    getEmptyEntries(subMap){
        const keys = new Array()
        for (const [key, value] of subMap){
            if (value.length == 0) keys.push(key)
        }
        return keys
    }
    popValues(subMap, values){
        for (const [key, value] of subMap){
            subMap.set(key, value.filter(val => !values.includes(val)))
        }
        return subMap
    }
    popEmptyEntries(subMap, keys){ // gets called after getEmptyEntries
        for( const k of keys){
            subMap.delete(k)
        }
    }
    getNodeFromVal(value){
        for(const node of this.nodes){
            if (node.value == value) return node
        }
        console.log("Didnt find the node...")
    }
    drawEdges(){
        this.ctx.strokeStyle = 'rgba(0, 0, 0, 1)'
        this.ctx.lineWidth = 2
        this.ctx.lineCap = 'round';
        for(const [key, arr] of this.map){
            const from_node = this.getNodeFromVal(key)
            for(const v of arr){
                const to_node = this.getNodeFromVal(v)
      
                function canvas_arrow( context, fromx, fromy, tox, toy ) {
                    const dx = tox - fromx;
                    const dy = toy - fromy;
                    const headlen = 12; // length of head in pixels
                    const angle = Math.atan2( dy, dx );
                    context.beginPath();
                    context.moveTo( fromx, fromy );
                    context.lineTo( tox, toy );
                    context.stroke();
                    context.beginPath();
                    context.moveTo( tox - headlen * Math.cos( angle - Math.PI / 6 ), toy - headlen * Math.sin( angle - Math.PI / 6 ) );
                    context.lineTo( tox, toy );
                    context.lineTo( tox - headlen * Math.cos( angle + Math.PI / 6 ), toy - headlen * Math.sin( angle + Math.PI / 6 ) );
                    context.stroke();
                }
                canvas_arrow(this.ctx, from_node.pos.x+10, from_node.pos.y - 20, to_node.pos.x+10, to_node.pos.y + 10);
            }
        }
    }
}