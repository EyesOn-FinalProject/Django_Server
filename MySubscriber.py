import paho.mqtt.client as mqtt
import requests, xmltodict, json
import pandas as pd
import paho.mqtt.publish as publish


"""
on_connect는 subscriber가 브로커에 연결하면서 호출할 함수
rc가 0이면 정상접속이 됐다는 의미
"""


def on_connect(client, userdata, flags, rc):
    print("connect.." + str(rc))
    if rc == 0:
        client.subscribe("eyeson/#")
    else:
        print("연결실패")


# 메시지가 도착됐을때 처리할 일들 - 여러가지 장비 제어하기, Mongodb에 저장
def on_message(client, userdata, msg):
    myval = msg.payload.decode("utf-8")
    myval = myval.split("/")
    id = myval[0]
    busNum = myval[1]
    latitude = myval[2]
    longitude = myval[3]

    # 공공 API 활용 KEY
    key = 'kNSQvU5WeosgTXwCx1mTthdz93%2BlLXHKA7ZtzbuNArBuUVVP4akW5xsfp6R5JYuMH106DwcuJRTqXJHI4q%2BNjA%3D%3D'

    # ===================================================================================================
    # 사용자의 위치를 받아서 tmX, tmY 변수에 지정 (우선 임시 지정)
    tmX = float(longitude)
    print(tmX)
    tmY = float(latitude)
    print(tmY)
    radius = 200 # 범위 (넓히면 여러 정류장 인식 됨.)

    def position(x, y, r) :
        url = f'http://ws.bus.go.kr/api/rest/stationinfo/getStationByPos?ServiceKey={key}&tmX={x}&tmY={y}&radius={r}'
        content = requests.get(url).content
        dict = xmltodict.parse(content)
        print(dict)

        # 첫번째 정류장이라 설정 (음성으로 "현재 인식된 정류장은 A 정류장 입니다." 라고 전해주기)
        print(1)
        target_stationName = str(dict['ServiceResult']['msgBody']['itemList'][0]['stationNm'])
        print(target_stationName)
        target_arsId = str(dict['ServiceResult']['msgBody']['itemList'][0]['arsId'])
        print(target_arsId)
        target_stId = int(dict['ServiceResult']['msgBody']['itemList'][0]['stationId'])
        print(target_stId)
        return (target_stId, target_stationName, target_arsId)

    station = position(tmX, tmY, radius)
    target_stId = station[0]
    target_stationName = station[1]
    target_arsId = station[2]

    print(target_stId)
    print(target_stationName)
    print(target_arsId)

    # ===================================================================================================
    data1 = pd.read_csv('data/busnumber_to_busRouteid.csv')
    print(data1)

    # 음성인식을 통해 사용자가 5714번 탄다고 가정했음.
    target_bus = '강남07' # str 형태로 해야됨. -> 엑셀이 str 형태

    def busnumber(target_bus) :
        target_busRouteId = data1[data1['busNumber'] == target_bus].iloc[0]['busRouteId']
        return(target_busRouteId)

    target_busRouteId = busnumber(target_bus)

    print(target_busRouteId)

    # ===================================================================================================
    # 서울특별시_노선정보조회 서비스 中 1_getStaionsByRouteList
    url = f'http://ws.bus.go.kr/api/rest/busRouteInfo/getStaionByRoute?ServiceKey={key}&busRouteId={target_busRouteId}'
    content = requests.get(url).content
    dict = xmltodict.parse(content)

    # target_arsId = arsId 넘버가 일치하는 버스의 seq(=ord) 구하기
    alist = []
    for i in range(0, len(dict['ServiceResult']['msgBody']['itemList'])):
        alist.append(dict['ServiceResult']['msgBody']['itemList'][i]['arsId'])

    # 인덱스 값이 곧 seq 값
    target_ord = alist.index(target_arsId) + 1
    print(target_ord)

    # ===================================================================================================
    # 서울특별시_버스도착정보조회 서비스 中 2_getArrInfoByRouteList
    url = f'http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRoute?ServiceKey=' \
        f'{key}&stId={target_stId}&busRouteId={target_busRouteId}&ord={target_ord}'

    content = requests.get(url).content
    dict = xmltodict.parse(content)

    arrival = json.dumps(dict['ServiceResult']['msgBody']['itemList']['arrmsg1'], ensure_ascii = False)
    arrival2 = json.dumps(dict['ServiceResult']['msgBody']['itemList']['arrmsg2'], ensure_ascii = False)
    busplainnum = json.dumps(dict['ServiceResult']['msgBody']['itemList']['plainNo1'], ensure_ascii = False)
    nextstation = json.dumps(dict['ServiceResult']['msgBody']['itemList']['stationNm1'], ensure_ascii = False)

    jsonarrival = json.loads(arrival) #첫번째 버스 도착예정시간
    jsonarrival2 = json.loads(arrival2) #두번째 버스 도착예정시간
    jsonbusplainnum = json.loads(busplainnum) #버스차량번호
    jsonnextstation = json.loads(nextstation) #다음 정류장

    publish.single("eyeson/busTime",jsonarrival,hostname = "15.164.46.54") #데이터 전송

mqttClient = mqtt.Client()  # 클라이언트 객체 생성
# 브로커에 연결이되면 내가 정의해놓은 on_connect함수가 실행되도록 등록
mqttClient.on_connect = on_connect

# 브로커에서 메시지가 전달되면 내가 등록해 놓은 on_message함수가 실행
mqttClient.on_message = on_message

# 브로커에 연결하기
mqttClient.connect("15.164.46.54", 1883, 60)

# 토픽이 전달될때까지 수신대기
mqttClient.loop_forever()
