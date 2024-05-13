$(function () {
    const test = true;
    let unit = '1';
    let ip = "127.0.0.1";
    let suffix = '';
    if (!test) {
        const currentUrl = window.location.href;
        ip = currentUrl.match(/(?:[0-9]{1,3}\.){3}[0-9]{1,3}/)[0];
        suffix = "_AVALUE";
    }

    if (localStorage.getItem('unit')) {
        unit = localStorage.getItem('unit');
    } else {
        localStorage.setItem('unit', unit);
    }
    if (!localStorage.getItem('titleText')) {
        localStorage.setItem('titleText', '抗燃油监测系统1');
    }
    $('.title-center').text(localStorage.getItem('titleText'));
    
    $('.title-center').click(function () {
        let currentText = $(this).text();

        if (currentText === '抗燃油监测系统1') {
            $(this).text('抗燃油监测系统2');
            unit = '2';
        } else if (currentText === '抗燃油监测系统2') {
            $(this).text('抗燃油监测系统1');
            unit = '1';
        }

        localStorage.setItem('unit', unit);
        localStorage.setItem('titleText', $(this).text());

        // 刷新页面
        location.reload();
    });

    let client = new Paho.Client(ip, 8083, "clientId");

    client.connect({
        onSuccess: function () {
            client.subscribe(`forecast-status1`);
            client.subscribe(`forecast-alerts1`);
            client.subscribe(`forecast-vr1`);
            client.subscribe(`forecast-health1`);
            client.subscribe(`points1`);

            client.subscribe(`forecast-status2`);
            client.subscribe(`forecast-alerts2`);
            client.subscribe(`forecast-vr2`);
            client.subscribe(`forecast-health2`);
            client.subscribe(`points2`);
        }
    });
    
    client.onMessageArrived = function (message) {
        let topic = message.destinationName;
        const titleText = localStorage.getItem('titleText');
        if (topic.slice(-1) !== titleText.slice(-1)) {
            // console.log(topic);
            return;
        }
        if (test) {
            unit = '1';
        }

        let payload = message.payloadString;
        let data = JSON.parse(payload);
        switch(topic.slice(0, -1)) {
            case `forecast-status`:
                // console.log("forecast-status", data);
                handleForecastStatus(data);
                break;
            case `forecast-alerts`:
                // console.log("forecast-alerts", data);
                handleForecastAlerts(data);
                break;
            case `forecast-vr`:
                // console.log("forecast-vr", data);
                handleForecastVR(data);
                break;
            case `forecast-health`:
                // console.log("forecast-health", data);
                handleForecastHealth(data);
                break;
            case `points`:
                // console.log("points", data);
                handlePoints(data);
                break;
            default:
                console.log("Unknown topic: " + topic);
        }
    };

    // alertList();

    const testNames = ["mba01cp151", "mba01cp152", "mba01cp153", "mba01cp154", "mba01cp155", "mba01cp156", "mba01cp157", "mba01cp158"];
    
    // 健康度
    let healthChart = echarts.init(document.getElementById('healthScore'));
    let currentTime = new Date();
    let heathTimeList = Array.from({ length: 10 }, (_, index) => {
        let time = new Date(currentTime.getTime() + index * 60 * 1000);
        let formattedTime = time.toLocaleTimeString();
        return formattedTime;
    });

    let healthOption = {
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        grid: {
            borderWidth: 30,
            top: 30,
            bottom: 20,
            left: 30,
            right: 30,
        },
        legend: {
            textStyle: {
                color: 'white',
                fontSize: 12
            }
        },
        toolbox: {
            show: true,
            orient: 'vertical',
            left: 'right',
            top: 'center',
            feature: {
                mark: { show: true },
                dataView: { show: true, readOnly: false },
                magicType: { show: true, type: ['line', 'bar', 'stack'] },
                restore: { show: true },
                saveAsImage: { show: true }
            }
        },
        xAxis: [
            {
                type: 'category',
                nameTextStyle: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisLabel: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisTick: { show: false },
                data: heathTimeList.map(time => time.toLocaleString())
            }
        ],
        yAxis: [
            {
                type: 'value',
                nameTextStyle: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisLabel: {
                    color: 'rgba(255, 255, 255, 1)',
                },
            }
        ],
        series: []
    };

    for (let i = 0; i < 1; i++) {
        healthOption.series[i] = {
            name: testNames[i],
            type: 'line',
            smooth: true,
            emphasis: {
                focus: 'series'
            },
            data: Array.from({ length: 30 }, (_, index) => index + 1),
            // markArea: { // 没效果
            //     itemStyle: {
            //         color: 'rgba(255, 255, 255,0.5)'
            //     },
            //     data: [
            //         [
            //             {
            //                 name: "forecast",
            //                 xAxis: heathTimeList[heathTimeList.length - 5].toLocaleString()
            //             },
            //             {
            //                 xAxis: heathTimeList[heathTimeList.length - 1].toLocaleString()
            //             }
            //         ]
            //     ]
            // }
        }
    }
    healthChart.setOption(healthOption);

    function handleForecastHealth(data) {
        timestamp = data["timestamp"];
        healthOption.xAxis[0].data = timestamp;
        healthOption.series[0] = {
            name: 'healthscore',
            type: 'line',
            smooth: true,
            emphasis: {
                focus: 'series'
            },
            data: data['healthscore']
        }
        healthChart.setOption(healthOption);
    }

    window.addEventListener("resize", function () {
        healthChart.resize();
    });

    // gif
    function handlePoints(data) {
        for (let i = 1; i < 5; ++i) {
            document.getElementById(`GSE00${i}VV`).textContent = data[`${unit}GSE0${i}4MP${suffix}`].toFixed(2) + "Mpa";
            document.getElementById(`GSE01${i}VV`).textContent = data[`${unit}GSE1${i}4MP${suffix}`].toFixed(2) + "Mpa";
            document.getElementById(`GRE00${i}VV`).textContent = data[`${unit}GRE0${i}6MP${suffix}`].toFixed(2) + "Mpa";
            document.getElementById(`GRE01${i}VV`).textContent = data[`${unit}GRE1${i}6MP${suffix}`].toFixed(2) + "Mpa";
        }

        for (let i = 1; i < 4; ++i) {
            document.getElementById(`GFR01${i}MP`).textContent = data[`${unit}GFR01${i}MP${suffix}`].toFixed(2) + "Mpa";
            document.getElementById(`GFR00${i}MN`).textContent = data[`${unit}GFR00${i}MN${suffix}`].toFixed(2) + "mm";
        }

        for (let i = 3; i < 5; ++i) {
            document.getElementById(`GFR00${2 * i}MT`).textContent = data[`${unit}GFR00${2* i}MT${suffix}`].toFixed(2) + "℃";
        }
    }

    // 报警列表
    function handleForecastAlerts(data) {
        const tableBody = document.querySelector('.data>.data-content>.con-right>.right-top>.table-container table tbody');
        let time = data["timestamp"];
        if (tableBody) {
            alarmPoints = [];
            tableBody.querySelectorAll('tr').forEach(row => {
                let cells = row.querySelectorAll('td');
                alarmPoints.push(cells[1].textContent);
                if (cells[3].textContent === '已确认') {
                    row.style.display = (hideAcknowledged) ? "none" : "";
                }
            });
            // tableBody.innerHTML = '';
            let alarms = data["alarms"];
            for (let i = 0; i < alarms.length; ++i) {
                const item = alarms[i];
                const { code, desc, advice, startTime } = item;
                if (!alarmPoints.includes(code)) {
                    const row = document.createElement('tr');

                    let level = '轻微';
                    if (level === '严重') {
                        row.classList.add('severity-high');
                    } else if (level === '中度') {
                        row.classList.add('severity-medium');
                    } else if (level === '轻微') {
                        row.classList.add('severity-low');
                    }

                    const timeCell = document.createElement('td');
                    timeCell.textContent = startTime;
                    row.appendChild(timeCell);

                    const nameCell = document.createElement('td');
                    nameCell.textContent = code;
                    row.appendChild(nameCell);

                    const levelCell = document.createElement('td');
                    levelCell.textContent = desc;
                    row.appendChild(levelCell);

                    const statusCell = document.createElement('td');
                    statusCell.textContent = '未确认';
                    row.addEventListener('dblclick', () => {
                        if (statusCell.textContent === '未确认') {
                            statusCell.textContent = '已确认';
                        }
                    });
                    row.appendChild(statusCell);

                    tableBody.appendChild(row);
                }
            }
        }
    }

    // 切换已确认可见性
    const clearAcknowledgement = document.getElementById('toggleVisibility');
    let hideAcknowledged = localStorage.getItem('hideAcknowledged') === 'true';
    clearAcknowledgement.innerHTML = (hideAcknowledged) ? "隐藏已确认" : "显示已确认";
    
    clearAcknowledgement.addEventListener('click', function () {
        hideAcknowledged = !hideAcknowledged;
        localStorage.setItem('hideAcknowledged', hideAcknowledged);
        clearAcknowledgement.innerHTML = (hideAcknowledged) ? "隐藏已确认" : "显示已确认";
    })

    // 系统状态
    let statusChart = echarts.init(document.getElementById('status'));

    let statusOption = {
        // legend: {
        //     left: 'right',
        //     textStyle: {
        //         color: 'white',
        //         fontSize: 12
        //     }
        // },
        tooltip: {
            trigger: 'item'
        },
        toolbox: {
            show: true,
            orient: 'vertical',
            left: 'right',
            top: 'bottom',
            feature: {
                mark: { show: true },
                dataView: { show: true, readOnly: false },
                saveAsImage: { show: true }
            }
        },
        series: [
            {
                type: 'pie',
                radius:  ['40%', '60%'],
                center: ['55%', '40%'],
                avoidLabelOverlap: false,
                data: [
                    { value: 1, name: '液位', itemStyle: {color: 'green'} },
                    { value: 1, name: '压力', itemStyle: {color: 'green'} },
                    { value: 1, name: '温度', itemStyle: {color: 'green'} },
                    { value: 1, name: '调节阀', itemStyle: {color: 'green'} },
                    { value: 1, name: '过滤器', itemStyle: {color: 'green'} },
                    { value: 1, name: '主汽阀', itemStyle: {color: 'green'} }
                ],
                emphasis: {
                    itemStyle: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)'
                    }
                }
            }
        ]
    }
    statusChart.setOption(statusOption);

    function handleForecastStatus(data) {
        const keys = Object.keys(data);
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const value = data[key];
            // console.log(statusOption.series[0].data[i].itemStyle.color);
            if (value === '异常') {
                statusOption.series[0].data[i].itemStyle.color = 'red';
            }
        }
        statusChart.setOption(statusOption);
    }

    // 液位,压力
    let liquidLevelChart = echarts.init(document.getElementById('liquidLevel'));
    let pressureChart = echarts.init(document.getElementById('pressure'));

    let timeList = Array.from({ length: 30 }, (_, index) => {
        let time = new Date(currentTime.getTime() + index * 60 * 1000);
        let formattedTime = time.toLocaleTimeString();
        return formattedTime;
    });

    let liquidLevelOption = {
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'shadow'
            }
        },
        grid: {
            borderWidth: 30,
            top: 30,
            bottom: 20,
            left: 30,
            right: 30,
        },
        legend: {
            textStyle: {
                color: 'white',
                fontSize: 12
            }
        },
        toolbox: {
            show: true,
            orient: 'vertical',
            left: 'right',
            top: 'center',
            feature: {
                mark: { show: true },
                dataView: { show: true, readOnly: false },
                magicType: { show: true, type: ['line', 'bar', 'stack'] },
                restore: { show: true },
                saveAsImage: { show: true }
            }
        },
        xAxis: [
            {
                type: 'category',
                nameTextStyle: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisLabel: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisTick: { show: false },
                data: timeList.map(time => time.toLocaleString())
            }
        ],
        yAxis: [
            {
                type: 'value',
                nameTextStyle: {
                    color: 'rgba(255, 255, 255, 1)',
                },
                axisLabel: {
                    color: 'rgba(255, 255, 255, 1)',
                },
            }
        ],
        series: []
    };

    for (let i = 0; i < 2; i++) {
        liquidLevelOption.series[i] = {
            name: testNames[i],
            type: 'line',
            smooth: true,
            emphasis: {
                focus: 'series'
            },
            data: Array.from({ length: 30 }, (_, index) => index + 1),
        }
    }
    liquidLevelChart.setOption(liquidLevelOption);

    let pressureOption = $.extend(true, {}, liquidLevelOption)
    pressureChart.setOption(pressureOption);

    // 温度，腔室油压
    let temperatureChart = echarts.init(document.getElementById('temperature'));
    let chamberPressureChart = echarts.init(document.getElementById('chamberPressure'));

    let temperatureOption = $.extend(true, {}, pressureOption);
    temperatureChart.setOption(temperatureOption);

    let chamberPressureOption = $.extend(true, {}, pressureOption);
    chamberPressureChart.setOption(chamberPressureOption);

    function handleForecastVR(data) {
        timestamp = data["timestamp"];
        liquidLevelOption.xAxis[0].data = timestamp;
        for (let i = 0; i < 3; i++) {
            liquidLevelOption.series[i] = {
                name: `${unit}GFR00${i + 1}MN`,
                type: 'line',
                smooth: true,
                emphasis: {
                    focus: 'series'
                },
                data: data[`${unit}GFR00${i + 1}MN${suffix}`]
            }
        }
        liquidLevelChart.setOption(liquidLevelOption);

        pressureOption.xAxis[0].data = timestamp;
        for (let i = 0; i < 3; i++) {
            pressureOption.series[i] = {
                name: `${unit}GFR01${i + 1}MP`,
                type: 'line',
                smooth: true,
                emphasis: {
                    focus: 'series'
                },
                data: data[`${unit}GFR01${i + 1}MP${suffix}`]
            }
        }
        pressureChart.setOption(pressureOption);

        temperatureOption.xAxis[0].data = timestamp;
        for (let i = 0; i < 2; i++) {
            temperatureOption.series[i] = {
                name: `${unit}GFR00${6 + 2 * i}MT`,
                type: 'line',
                smooth: true,
                emphasis: {
                    focus: 'series'
                },
                data: data[`${unit}GFR00${6 + 2 * i}MT${suffix}`]
            }
        }
        temperatureChart.setOption(temperatureOption);

        chamberPressureOption.xAxis[0].data = timestamp;
        for (let i = 0; i < 4; i++) {
            chamberPressureOption.series[i] = {
                name: `${unit}GRE0${i + 1}6MP`,
                type: 'line',
                smooth: true,
                emphasis: {
                    focus: 'series'
                },
                data: data[`${unit}GRE0${i + 1}6MP${suffix}`]
            }
        }
        for (let i = 0; i < 4; i++) {
            chamberPressureOption.series[i + 4] = {
                name: `${unit}GRE1${i + 1}6MP`,
                type: 'line',
                smooth: true,
                emphasis: {
                    focus: 'series'
                },
                data: data[`${unit}GRE1${i + 1}6MP${suffix}`]
            }
        }
        chamberPressureChart.setOption(chamberPressureOption);
    }
    
    // 监听窗口大小变化事件，使图表自适应窗口大小
    window.addEventListener('resize', function () {
        liquidLevelChart.resize();
        pressureChart.resize();
        temperatureChart.resize();
        chamberPressureChart.resize();
    });
})
