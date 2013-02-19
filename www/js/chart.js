// Create a function that returns a particular property of its parameter.
// If that property is a function, invoke it (and pass optional params).
function ƒ(name){
  var v,params=Array.prototype.slice.call(arguments,1);
  return function(o){
    return (typeof (v=o[name])==='function' ? v.apply(o,params) : v );
  };
}

// Return the first argument passed in
function I(d){ return d }

var g = g || {};

g.Chart = function(){
    return {
        url : "data/data.json",
        $j : jQuery,
        //defaults
        width           : 970,
        height          : 850,
        groupPadding    : 10,
        totalValue      : 3700000000,
        deficitValue    : 901000000,
        // CONST
        MANDATORY       : "Mandatory",
        DISCRETIONARY   : "Discretionary",
        NET_INTEREST    : "Net interest",

        //will be calculated later
        boundingRadius  : null,
        maxRadius       : null,
        centerX         : null,
        centerY         : null,
        scatterPlotY    : null,

        //d3 settings
        defaultGravity  : 0.1,
        defaultCharge   : function(d){
            if (d.value < 0) {
                return 0
            } else {
                return -Math.pow(d.radius,2.0)/8
            };
        },
        links           : [],
        nodes           : [],
        positiveNodes   : [],
        force           : {},
        svg             : {},
        circle          : {},
        gravity         : null,
        charge          : null,
        changeTickValues: [-0.25, -0.15, -0.05, 0.05, 0.15, 0.25],
        categorizeChange: function(c){
            if (isNaN(c)) { return 0;
                          } else if ( c < -0.25) { return -3;
                                                 } else if ( c < -0.05){ return -2;
                                                                       } else if ( c < -0.001){ return -1;
                                                                                              } else if ( c <= 0.001){ return 0;
                                                                                                                     } else if ( c <= 0.05){ return 1;
                                                                                                                                           } else if ( c <= 0.25){ return 2;
                                                                                                                                                                 } else { return 3; }
        },
        fillColor       : d3.scale.ordinal().domain([-3,-2,-1,0,1,2,3]).range(["#d84b2a", "#ee9586","#e4b7b2","#AAA","#beccae", "#9caf84", "#7aa25c"]),
        strokeColor     : d3.scale.ordinal().domain([-3,-2,-1,0,1,2,3]).range(["#c72d0a", "#e67761","#d9a097","#999","#a7bb8f", "#7e965d", "#5a8731"]),
        getFillColor    : null,
        getStrokeColor  : null,
        pFormat         : d3.format("+.1%"),
        pctFormat       : function(){return false},
        tickChangeFormat: d3.format("+%"),
        simpleFormat    : d3.format(","),
        simpleDecimal   : d3.format(",.2f"),

        bigFormat       : function(n){return g.formatNumber(n*1000)},
        nameFormat      : function(n){return n},
        discretionFormat: function(d){
            if (d == 'Discretionary' || d == 'Mandatory') {
                return d + " spending"
            } else {return d}
        },

        rScale          : d3.scale.pow().exponent(0.5).domain([0,1000000000]).range([1,90]),
        radiusScale     : null,
        changeScale     : d3.scale.linear().domain([-0.28,0.28]).range([620,180]).clamp(true),
        sizeScale       : d3.scale.linear().domain([0,110]).range([0,1]),
        groupScale      : {},

        //data settings
        currentYearDataColumn   : 'budget_2013',
        previousYearDataColumn  : 'budget_2012',
        data                    : g.budget_array_data,
        categoryPositionLookup  : {},
        categoriesList          : [],

        //
        //
        //
        init: function() {
            var that = this;

            this.scatterPlotY = this.changeScale(0);

            this.pctFormat = function(p){
                if (p === Infinity ||p === -Infinity) {
                    return "N.A."
                } else {
                    return that.pFormat(p)
                }

            }

            this.radiusScale = function(n){ return that.rScale(Math.abs(n)); };
            this.getStrokeColor = function(d){
                // if (d.isNegative) {
                //   return "#333"
                // }
                return that.strokeColor(d.changeCategory);
            };
            this.getFillColor = function(d){
                if (d.isNegative) {
                    return "#fff"
                }
                return that.fillColor(d.changeCategory);
            };

            this.boundingRadius = this.radiusScale(this.totalValue);
            this.centerX = this.width / 2;
            this.centerY = 300;

            //
            //this.groupScale = d3.scale.ordinal()
            //    .domain(this.categoriesList)
            //    .rangePoints([0,1]);

            this.getData();

        },

        getData : function() {
            var that = this;
            d3.json(that.url, function(error, json) {
                if( error ) return console.warn(error);
                var firstp = (!that.data || that.data.length == 0);
                that.data = json;
                that.processData();
                that.render();
                if( firstp ) {
                    that.start();
                    that.totalLayout();
                } else {
                    that.force.start();
                }
                that.dev(2);
            });
        },

        processData : function() {
            var that = this;

            // Builds the nodes data array from the original data
            for (var i=0; i < this.data.length; i++) {
                var n = this.data[i];
                var out = {
//                    id: n['id'],
                    id: i,
                    radius: this.radiusScale(n[this.currentYearDataColumn]),
//                    group: n['department'],
//                    change: n['change'],
                    changeCategory: this.categorizeChange(n['change']),
                    value: n[this.currentYearDataColumn],
                    name: n['name'],
//                    discretion: n['discretion'],
                    isNegative: (n[this.currentYearDataColumn] < 0),
//                    positions: n.positions,
                    x:Math.random() * 1000,
                    y:Math.random() * 1000
                }
                if (n.positions.total) {
                    out.x = n.positions.total.x +
                        (n.positions.total.x - (that.width / 2)) * 0.5;
                    out.y = n.positions.total.y +
                        (n.positions.total.y - (150)) * 0.5;
                };
                if ((n[this.currentYearDataColumn] > 0) !==
                    (n[this.previousYearDataColumn] > 0)) {
                    out.change = "N.A.";
                    out.changeCategory = 0;
                };

                this.nodes[i] = out;
            };

        },

        render : function() {
            var that = this;

            var svg = d3.select("#chart").selectAll("svg").data([0]);
            svg.enter().append("svg")
                .attr("width", this.width);

            var circle = svg.selectAll("circle").data(that.nodes, ƒ('id'));

            circle.enter().append("circle")
                .style("fill", function(d) { return that.getFillColor(d); } )
                .style("stroke-width", 1)
//                .attr('id',function(d) { return 'g-circle'+d.id })
                .style("stroke", function(d){ return that.getStrokeColor(d); });

            circle.transition().duration(1000)
                .attr("r", function(d){return that.radiusScale(d.value)});

            circle.exit().transition().duration(300)
                .attr('opacity',0)
                .remove();

            this.svg = svg;
            this.circle = circle;

/*  used to be on enter()
                .on("mouseover",function(d,i) {
                    var el = d3.select(this)
                    var xpos = Number(el.attr('cx'))
                    var ypos = (el.attr('cy') - d.radius - 10)
                    el.style("stroke","#000").style("stroke-width",3);
                    d3.select("#g-tooltip")
                        .style('top',ypos+"px")
                        .style('left',xpos+"px").style('display','block')
                        .classed('g-plus', (d.changeCategory > 0))
                        .classed('g-minus', (d.changeCategory < 0));
                    d3.select("#g-tooltip .g-name").
                        html(that.nameFormat(d.name))

                    d3.select("#g-tooltip .g-discretion")
                        .text(that.discretionFormat(d.discretion))
                    d3.select("#g-tooltip .g-department").text(d.group)
                    d3.select("#g-tooltip .g-value")
                        .html("$"+that.bigFormat(d.value))

                    var pctchngout = that.pctFormat(d.change)
                    if (d.change == "N.A.") {
                        pctchngout = "N.A."
                    };
                    d3.select("#g-tooltip .g-change").html(pctchngout) })
                .on("mouseout",function(d,i) {
                    d3.select(this)
                        .style("stroke-width",1)
                        .style("stroke", function(d){
                            return that.getStrokeColor(d);
                        })
                    d3.select("#g-tooltip").style('display','none')});
*/

        },

        //
        //
        //
        getCirclePositions: function(){
            var that = this
            var circlePositions = {};
            this.circle.each(function(d){

                circlePositions[d.id] = {
                    x:Math.round(d.x),
                    y:Math.round(d.y)
                }


            })
                return JSON.stringify(circlePositions)
        },



        //
        //
        //
        start: function() {
            var that = this;
            this.force = d3.layout.force()
                .nodes(this.nodes)
                .size([this.width, this.height]);
        },




        //
        //
        //
        totalLayout: function() {
            var that = this;
            this.force
                .gravity(-0.01)
                .charge(that.defaultCharge)
                .friction(0.9)
                .on("tick", function(e){
                    that.circle
                        .each(that.totalSort(e.alpha))
                            .each(that.buoyancy(e.alpha))
                                .attr("cx", function(d) { return d.x; })
                        .attr("cy", function(d) { return d.y; });
                })
                .start();

        },

        // --------------------------------------------------------------------
        // FORCES
        // --------------------------------------------------------------------


        //
        //
        //
        totalSort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.centerY;
                var targetX = that.width / 2;


                if (d.isNegative) {
                    if (d.changeCategory > 0) {
                        d.x = - 200
                    } else {
                        d.x =  1100
                    }
                }

                // if (d.positions.total) {
                //   targetX = d.positions.total.x
                //   targetY = d.positions.total.y
                // };



                //
                d.y = d.y + (targetY - d.y) * (that.defaultGravity + 0.02) * alpha
                d.x = d.x + (targetX - d.x) * (that.defaultGravity + 0.02) * alpha

            };
        },

        //
        //
        //
        buoyancy: function(alpha) {
            var that = this;
            return function(d){
                // d.y -= 1000 * alpha * alpha * alpha * d.changeCategory

                // if (d.changeCategory >= 0) {
                //   d.y -= 1000 * alpha * alpha * alpha
                // } else {
                //   d.y += 1000 * alpha * alpha * alpha
                // }


                var targetY = that.centerY - (d.changeCategory / 3) * that.boundingRadius
                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha * alpha * alpha * 100



            };
        },



        //
        //
        //
        mandatorySort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.centerY;
                var targetX = 0;

                if (d.isNegative) {
                    if (d.changeCategory > 0) {
                        d.x = - 200
                    } else {
                        d.x =  1100
                    }
                    return;
                }


                if (d.discretion === that.DISCRETIONARY) {
                    targetX = 550
                } else if ((d.discretion === that.MANDATORY)||(d.discretion === that.NET_INTEREST)) {
                    targetX = 400
                } else {
                    targetX = 900
                };




                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha * 1.1
                d.x = d.x + (targetX - d.x) * (that.defaultGravity) * alpha * 1.1
            };
        },


        //
        //
        //
        discretionarySort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.height / 2;
                var targetX = 0;

                if (d.isNegative) {
                    if (d.changeCategory > 0) {
                        d.x = - 200
                    } else {
                        d.x =  1100
                    }
                    return;
                }


                if (d.discretion === "Discretionary") {
                    targetY = that.changeScale(d.change);
                    targetX = 100 + that.groupScale(d.group)*(that.width - 120);
                    if (isNaN(targetY)) {targetY = that.centerY};
                    if (targetY > (that.height-80)) {targetY = that.height-80};
                    if (targetY < 80) {targetY = 80};

                } else if ((d.discretion === "Mandatory")||(d.discretion === "Net interest")) {
                    targetX = -300 + Math.random()* 100;
                    targetY = d.y;
                } else {
                    targetX = 0
                };

                d.y = d.y + (targetY - d.y) * Math.sin(Math.PI * (1 - alpha*10)) * 0.2
                d.x = d.x + (targetX - d.x) * Math.sin(Math.PI * (1 - alpha*10)) * 0.1
            };
        },


        //
        //
        //
        departmentSort: function(alpha){
            var that = this;
            return function(d){
                var targetY = 0,
                targetX = 0;

                if (that.categoryPositionLookup[d.group]) {
                    targetY = that.categoryPositionLookup[d.group].y;
                    targetX = that.categoryPositionLookup[d.group].x;
                } else {
                };


                var r =  Math.max(5, d.radius)
                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha * 0.5 * r
                d.x = d.x + (targetX - d.x) * (that.defaultGravity) * alpha * 0.5 * r

            };
        },




        //
        //
        //
        staticDepartment: function(alpha) {
            var that = this;
            return function(d){
                var targetY = 0;
                var targetX = 0;

                if (d.positions.department) {
                    targetX = d.positions.department.x;
                    targetY = d.positions.department.y;
                };

                d.y += (targetY - d.y) * Math.sin(Math.PI * (1 - alpha*10)) * 0.6
                d.x += (targetX - d.x) * Math.sin(Math.PI * (1 - alpha*10)) * 0.4
            };
        },

        //
        //
        //
        comparisonSort: function(alpha) {
            var that = this;
            return function(d){
                var targetY = that.height / 2;
                var targetX = 650;


                d.y = d.y + (targetY - d.y) * (that.defaultGravity) * alpha
                d.x = d.x + (targetX - d.x) * (that.defaultGravity) * alpha
            };
        },



        //
        //
        //
        collide: function(alpha){
            var that = this;
            var padding = 6;
            var quadtree = d3.geom.quadtree(this.nodes);
            return function(d) {
                var r = d.radius + that.maxRadius + padding,
                nx1 = d.x - r,
                nx2 = d.x + r,
                ny1 = d.y - r,
                ny2 = d.y + r;
                quadtree.visit(function(quad, x1, y1, x2, y2) {
                    if (quad.point && (quad.point !== d) && (d.group === quad.point.group)) {
                        var x = d.x - quad.point.x,
                        y = d.y - quad.point.y,
                        l = Math.sqrt(x * x + y * y),
                        r = d.radius + quad.point.radius;
                        if (l < r) {
                            l = (l - r) / l * alpha;
                            d.x -= x *= l;
                            d.y -= y *= l;
                            quad.point.x += x;
                            quad.point.y += y;
                        }
                    }
                    return x1 > nx2
                        || x2 < nx1
                        || y1 > ny2
                        || y2 < ny1;
                });
            };

        },

        // for dev only
        dev : function(wait) {
            var that = this;
            console.log(g.c.nodes[0]);
            setTimeout(function() {
                g.c.nodes[0].value = 1075650000;
                g.c.nodes[0].radius = that.radiusScale(g.c.nodes[0].value);
                console.log(g.c.nodes[0]);
            }, wait * 1000);

        }

    }
};

g.ready = function() {
    g.c = new g.Chart();
    g.c.init();
}

if (!!document.createElementNS && !!document.createElementNS('http://www.w3.org/2000/svg', "svg").createSVGRect){
    $(document).ready($.proxy(g.ready, this));
} else {
    $("#container").hide();
}
