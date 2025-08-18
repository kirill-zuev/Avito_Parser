import 'package:flutter/material.dart';
import 'package:fl_chart/fl_chart.dart';
import 'dart:convert';
import 'package:http/http.dart' as http;

void main() => runApp(const MyApp());

class MyApp extends StatelessWidget {
  const MyApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Avito',
      theme: ThemeData(primarySwatch: Colors.blue),
      debugShowCheckedModeBanner: false,
      home: const ChartPage(),
    );
  }
}

class ChartPage extends StatefulWidget {
  const ChartPage({super.key});
  @override
  _ChartPageState createState() => _ChartPageState();
}

class _ChartPageState extends State<ChartPage> {
  String? selectedTable;
  List<String> tables = [];
  List<Map<String, dynamic>> chartData = [];

  @override
  void initState() {
    super.initState();
    fetchTables();
  }

  Future<void> fetchTables() async {
    final res = await http.get(Uri.parse('http://localhost:9005/api/tables'));
    final list = jsonDecode(res.body);
    setState(() => tables = List<String>.from(list));
    if (tables.isNotEmpty) selectedTable = tables[0];
    fetchData();
  }

  Future<void> fetchData() async {
    if (selectedTable == null) return;
    final res = await http.get(Uri.parse(
        'http://localhost:9005/api/data?table=$selectedTable'));
    final list = jsonDecode(res.body);
    setState(() => chartData = List<Map<String, dynamic>>.from(list));
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Avito')),
      body: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(children: [
          Row(
            children: [
              const SizedBox(width: 16),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 0),
                decoration: BoxDecoration(
                  color: Colors.white,
                  borderRadius: BorderRadius.circular(12),
                  boxShadow: [
                    BoxShadow(
                      color: Colors.black12,
                      blurRadius: 6,
                      offset: Offset(0, 5),
                    ),
                  ],
                ),
                child: DropdownButtonHideUnderline(
                  child: DropdownButton<String>(
                    value: selectedTable,
                    items: tables
                        .map((e) => DropdownMenuItem(
                              value: e,
                              child: Text(e),
                            ))
                        .toList(),
                    onChanged: (val) {
                      setState(() => selectedTable = val);
                    },
                    icon: Icon(Icons.arrow_drop_down, color: Colors.blue),
                    style: TextStyle(
                      color: Colors.black87,
                      fontSize: 16,
                    ),
                    dropdownColor: Colors.white,
                  ),
                ),
              ),
              const SizedBox(width: 15),
              ElevatedButton(
                onPressed: fetchData,
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.white,
                  foregroundColor: Colors.black,
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(12),
                  ),
                  padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 16),
                ),
                child: const Text('Обновить график'),
              ),
            ],
          ),
          const SizedBox(height: 15),
          Expanded(
            child: Center(
              child: Container(
                width: MediaQuery.of(context).size.width * 1,
                margin: const EdgeInsets.symmetric(horizontal: 0),
                child: chartData.isEmpty
                    ? const Center(child: Text('Нет данных'))
                    : LineChart(
                    LineChartData(
                      lineTouchData: LineTouchData(
                        enabled: false,
                      ),
                      gridData: FlGridData(
                        drawVerticalLine: true,
                        drawHorizontalLine: true,
                        verticalInterval: 1,
                        horizontalInterval: 1,
                        getDrawingVerticalLine: (value) {
                          return FlLine(
                            color: Colors.grey,
                            strokeWidth: 0.5,
                            dashArray: null,
                          );
                        },
                        getDrawingHorizontalLine: (value) {
                          final existsY = chartData.any(
                            (item) =>
                                (item['avg_price'] as num).toInt() == value.toInt(),
                          );
                          if (!existsY) return FlLine(color: Colors.transparent);
                          return FlLine(color: Colors.grey, strokeWidth: 0.5);
                        },
                      ),
                      titlesData: FlTitlesData(
                        bottomTitles: AxisTitles(
                          sideTitles: SideTitles(
                            showTitles: true,
                            interval: 1,
                            getTitlesWidget: (value, meta) {
                              int idx = value.toInt();
                              if (idx < 0 || idx >= chartData.length) return const SizedBox();
                              return Text('${chartData[idx]['days']}');
                            },
                          ),
                        ),
                        leftTitles: AxisTitles(
                          sideTitles: SideTitles(
                            showTitles: true,
                            interval: 1,
                            getTitlesWidget: (value, meta) {
                              final hasPoint = chartData.any(
                                (item) =>
                                    (item['avg_price'] as num).toInt() == value.toInt(),
                              );
                              if (!hasPoint) return const SizedBox();
                              return SizedBox(
                                child: FittedBox(
                                  child: Text(
                                    '${value.toInt()} ',
                                    textAlign: TextAlign.center,
                                    style: const TextStyle(
                                      fontWeight: FontWeight.bold,
                                      color: Colors.black,
                                    ),
                                  ),
                                ),
                              );
                            },
                          ),
                        ),
                        topTitles: AxisTitles(sideTitles: SideTitles(showTitles: false)),
                        rightTitles: AxisTitles(sideTitles: SideTitles(showTitles: false)),
                      ),
                      lineBarsData: [
                        LineChartBarData(
                          isCurved: false,
                          barWidth: 4,
                          color: Colors.blue,
                          spots: List.generate(chartData.length, (i) {
                            return FlSpot(
                              i.toDouble(),
                              (chartData[i]['avg_price'] as num).toDouble(),
                            );
                          }),
                          dotData: FlDotData(show: true),
                        ),
                      ],
                    ),
                  ),
              )
            ),
          ),
        ]),
      ),
    );
  }
}
