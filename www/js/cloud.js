var g = g || {};

g.Cloud = function(){
    return {
        delay : 2000,
        url : "data/users.json",
        $j : jQuery,

        users : [],
        tags : [],
        fontsize : null,

        svg : null,
        vis : null,
        background : null,

        //defaults
        width           : 400,
        height          : 500,
        totalValue      : 0,
        max             : 200,
        changed         : false,

        fill : d3.scale.category20(),

        init : function() {
            var that = this;

            that.svg = d3.select("#users").selectAll("svg").data([0]);
            that.svg.enter().append("svg")
                .attr("width", that.width);

            that.background = that.svg.append("g");

            that.vis = that.svg.append("g")
                .attr("transform", "translate("
                      + [that.width >> 1, that.height >> 1] + ")");

            that.getData(function() {
                that.generate();
                that.loop();
            });

        },

        progress : function(d) {},

        getData : function(callback) {
            var that = this;

            $.ajax({
                url : that.url,
                dataType : "json",
                cache : false,
                success : function(json, status, jqXHR) {
                    that.data = json;
                    that.processData();
                    that.parseText();
                    callback();
                },
                error : function(jqXHR, status, error) {
                    that.data = {};
                    callback();
                }
            });
        },

        processData : function() {
            var that = this;
            var old_users = that.users;

            var users = [];
            $.each( that.data, function(user, cnt) {
                for( var i=0; i < cnt; i++ ) {
                    users.push(user);
                }
            });
            that.users = users.join(" ");
            if( that.users != old_users ) {
                that.changed = true;
            }
        },

        parseText : function() {
            var that = this;
            that.tags = [];
            var cases = {};
            var separators = /[\s\u3031-\u3035\u309b\u309c\u30a0\u30fc\uff70]+/g;
            that.users.split(separators).forEach(function(word) {
                //if (discard.test(word)) return;
                //word = word.replace(punctuation, "");
                //if (stopWords.test(word.toLowerCase())) return;
                //if (word.length <= 3) return;
                //word = word.substr(0, maxLength);
                cases[word] = word;
                that.tags[word] = (that.tags[word] || 0) + 1;
            });
            that.tags = d3.entries(that.tags).sort(function(a, b) {
                return b.value - a.value;
            });
            that.tags.forEach(function(d) { d.key = cases[d.key]; });
        },

        generate : function() {
            var that = this;

            that.layout = d3.layout.cloud()
                .font("Helvetica")
                //.spiral("archimedean")
                .spiral("rectangular")
                .timeInterval(10)
                .size([that.width, that.height])
                .fontSize(function(d) { return that.fontSize(+d.value); })
                .text(function(d) { return d.key; })
                //.rotate(function(d) { return ~~(Math.random()*8) * 15 - 60; })
                .rotate(function(d,i) { return i & 1 ? 90 : 0; })
                .on("word", that.progress)
                .on("end", that.render);

            that.fontSize = d3.scale["linear"]().range([10,50]);
            if( that.tags.length ) {
                that.fontSize.domain([that.tags[that.tags.length - 1].value
                                      || 1, that.tags[0].value]);
                that.changed = true;
                that.update();
            }
        },

        update : function() {
            var that = this;

            if( that.changed ) {
                that.layout
                    .stop()
                    .words(that.tags
                           .slice(0, Math.min(that.tags.length, that.max)))
                    .start();
                that.changed = false;
            }
        },

        loop : function() {
            var that = this;
            that.loop_interval = setInterval( function() {
                that.getData(function() { that.update() });
            }, that.delay);
        },

        pause : function() {
            var that = this;
            clearInterval(that.loop_interval);
        },

        render : function(data, bounds) {
            var that = g.u; // seems like a hack to not use 'this'
            var w = that.width, h = that.height;

            var scale = bounds ? Math.min(
                w / Math.abs(bounds[1].x - w / 2),
                w / Math.abs(bounds[0].x - w / 2),
                h / Math.abs(bounds[1].y - h / 2),
                h / Math.abs(bounds[0].y - h / 2)) / 2 : 1;

            var text = that.vis.selectAll("text")
                .data(data, function(d) {
                    return d.text;
                });

            text.transition()
                .duration(1000)
                .attr("transform", function(d) {
                    return "translate(" + [d.x, d.y]
                        + ")rotate(" + d.rotate + ")";
                })
                .style("font-size", function(d) { return d.size + "px"; });
            text.enter().append("text")
                .attr("text-anchor", "middle")
                .attr("transform", function(d) {
                    return "translate(" + [d.x, d.y]
                        + ")rotate(" + d.rotate + ")";
                })
                .style("font-size", function(d) { return d.size + "px"; })
                .style("opacity", 1e-6)
                .style("stroke-width", 1)
                .style("stroke", function(d) {return that.fill(d.text); })
                .on("mouseover", function(d,i) {
                    var el = d3.select(this);
                    el.style("stroke","#fff");
                })
                .on("mouseout", function(d,i) {
                    var el = d3.select(this);
                    el.style("stroke", function(d) {return that.fill(d.text); })
                })
                .transition()
                .duration(1000)
                .style("opacity", 1);
            text.style("font-family", function(d) { return d.font; })
                .style("fill", function(d) { return that.fill(d.text); })
                .text(function(d) { return d.text; });
            var exitGroup = that.background.append("g")
                .attr("transform", that.vis.attr("transform"));
            var exitGroupNode = exitGroup.node();
            text.exit().each(function() {
                exitGroupNode.appendChild(this);
            });
            exitGroup.transition()
                .duration(1000)
                .style("opacity", 1e-6)
                .remove();
            that.vis.transition()
                .delay(1000)
                .duration(750)
                .attr("transform", "translate(" + [w >> 1, h >> 1] + ")scale("
                      + scale + ")");

        }

    }

}
