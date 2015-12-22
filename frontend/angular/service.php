<?php

  $urlParams = explode('?', $_SERVER['REQUEST_URI']);
  $functionName = $urlParams[2];
  print $functionName;

  $curl_h = curl_init('http://104.196.37.110:20550/slafka_daily/*');

  curl_setopt($curl_h, CURLOPT_HTTPHEADER,
    array('Accept: application/json')
  );

  # do not output, but store to variable
  curl_setopt($curl_h, CURLOPT_RETURNTRANSFER, true);

  $response = json_decode(curl_exec($curl_h));
  $counter = 0;
  foreach($response->Row as $row) {
      //print_r($row);
      $output_obj[$counter]['messageDate'] = base64_decode($row->key);
      foreach($row->Cell as $cell) {
        $output_obj[$counter][str_replace('tsa:','',base64_decode($cell->column))] = base64_decode($cell->{'$'});
      }
      $counter++;
  }
  print_r(json_encode($output_obj));
?>
