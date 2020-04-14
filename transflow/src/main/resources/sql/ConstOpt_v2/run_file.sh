#!/bin/bash

OLDDIR=$(pwd)
THEDIR="$(dirname ${0})/"
cd ${THEDIR}
THEDIR=$(pwd)
echo "原始目录：${OLDDIR}  ==== 当前目录：  ${THEDIR}"

DATE_S=$(date "+%Y-%m-%d %H:%M:%S")
echo "===========    开始时间：${DATE_S}    ==========="
echo "参数1 ${1}"     # shell file
echo "参数2 ${2}"     # SQL file
echo "参数3 ${3}"     # step  day / month / year
echo "参数4 ${4}"     # start date
echo "参数5 ${5}"     # end date


if [[ ${#} -lt 2 ]]
then
    echo "参数不足"
    exit -1
fi


if [[ -f "${OLDDIR}/${1}" ]]
then
    SHELL_F="${OLDDIR}/${1}"
elif [[ -f "${THEDIR}/${1}" ]]
then
    SHELL_F="${THEDIR}/${1}"
else
    cd ${OLDDIR}
    echo "===========    没有找到执行文件： ${1}"
    exit -2
fi
echo "===========    找到执行文件： ${SHELL_F}"


SQL_F="${2}"
if [[ -f "${THEDIR}/${2}" ]]
then
    cd ${THEDIR}
    echo "===========    找到SQL文件: ${THEDIR}/${2}  ,pwd=$(pwd) , 文件名：${SQL_F}"
elif [[ -f "${OLDDIR}/${2}" ]]
then
    cd ${OLDDIR}
    echo "===========    找到SQL文件: ${OLDDIR}/${2}  ,pwd=$(pwd) , 文件名：${SQL_F}"
else
    cd ${OLDDIR}
    echo "===========    没有找到SQL文件：${SQL_F}"
#    exit -3
fi

ONE_LOOP=0
STEP="${3}"
if [[ "${3}" = "DAY" || "${3}" = "day" || "${3}" = "MONTH" || "${3}" = "month" || "${3}" = "year" || "${3}" = "YEAR" ]]
then
    echo "===========    步长设定为: ${STEP}"
elif [[ "${3}" = "ONE" || "${3}" = "one" || -z "${3}" ]]
then
    echo "===========    步长设定为空: ${STEP}"
    ONE_LOOP=1
else
    cd ${OLDDIR}
    echo "===========    步长设定错误: ${STEP}"
    exit -4
fi


if [[ -z "${4}" ]]
then
    START_DAY=$(date -d "-10 day" "+%Y-%m-%d")
    ONE_LOOP=1
else
    START_DAY=${4}
fi

if [[ -z "${5}" ]]
then
    END_DAY=$(date -d "+2 day" "+%Y-%m-%d")
else
    END_DAY=${5}
fi


START_DAY=$(date -d "${START_DAY}" "+%Y-%m-%d")
res1=${?}
END_DAY=$(date -d "${END_DAY}" "+%Y-%m-%d")
res2=${?}
if [[ ${res1} -ne 0 || ${res2} -ne 0 ]]
then
    cd ${OLDDIR}
    echo "===========    日期格式不对："
    exit -5
fi

# START_DAY ="2017-10-01"
# END_DAY   ="2020-04-01"
# 包括开始日期，不包括结束日期


if [[ ${ONE_LOOP} -gt 0 ]]
then

    echo -e "\n\n\n一次处理日期  sh \"${SHELL_F}\" \"${SQL_F}\" \"${START_DAY}\" \"${END_DAY}\"    ***********************"
    sh "${SHELL_F}" "${SQL_F}" "${START_DAY}" "${END_DAY}"

else
    for ((i=0;;i++))
    do
        DATE_L1=$(date -d "${START_DAY} +${i} ${STEP}" "+%Y-%m-%d")
        DATE_L2=$(date -d "${START_DAY} +`expr ${i} + 1` ${STEP}" "+%Y-%m-%d")

        if [[ "$(date -d "${DATE_L1}" "+%s")" -ge "$(date -d "${END_DAY}" "+%s")" ]]
        then
            echo -e "\n\n\n${DATE_L1} ，跳出"
            break
        else
            echo -e "\n\n\n${DATE_L1} ，继续"
        fi


        echo "处理日期 ${i} sh \"${SHELL_F}\" \"${SQL_F}\" \"${DATE_L1}\" \"${DATE_L2}\"    ***********************"
        sh "${SHELL_F}" "${SQL_F}" "${DATE_L1}" "${DATE_L2}"
    done
fi

echo "===========    开始时间：${DATE_S}    ==========="
echo "===========    结束时间：$(date "+%Y-%m-%d %H:%M:%S")    ==========="
