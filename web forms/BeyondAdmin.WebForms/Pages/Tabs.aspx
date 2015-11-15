<%@ Page Title="" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Tabs.aspx.cs" Inherits="BeyondAdmin.WebForms.Pages.Tabs" %>

<asp:Content ID="Main" ContentPlaceHolderID="MainContent" runat="server">
    <div class="row">
        <div class="col-lg-12 col-sm-12 col-xs-12">
            <h5 class="row-title"><i class="typcn typcn-folder themeprimary"></i>Simple Tabs</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable">
                        <ul class="nav nav-tabs" id="myTab">
                            <li class="active">
                                <a data-toggle="tab" href="#home">Home
                                </a>
                            </li>

                            <li class="tab-red">
                                <a data-toggle="tab" href="#profile">Profile
                                </a>
                            </li>

                            <li class="dropdown">
                                <a data-toggle="dropdown" class="dropdown-toggle" href="#">Dropdown
                                                    <b class="caret"></b>
                                </a>

                                <ul class="dropdown-menu dropdown-blue">
                                    <li>
                                        <a data-toggle="tab" href="#dropdown1">@fat</a>
                                    </li>

                                    <li>
                                        <a data-toggle="tab" href="#dropdown2">@mdo</a>
                                    </li>
                                </ul>
                            </li>
                        </ul>

                        <div class="tab-content">
                            <div id="home" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown1" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>

                            <div id="dropdown2" class="tab-pane">
                                <p>Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                            </div>
                        </div>
                    </div>
                    <div class="horizontal-space"></div>

                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable tabs-below">
                        <div class="tab-content">
                            <div id="home2" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile2" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown12" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>
                        </div>

                        <ul class="nav nav-tabs" id="myTab2">
                            <li class="active">
                                <a data-toggle="tab" href="#home2">Home</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#profile2">Profile</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#dropdown12">More</a>
                            </li>
                        </ul>
                    </div>
                    <div class="horizontal-space"></div>

                </div>
            </div>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable tabs-left">
                        <ul class="nav nav-tabs" id="myTab3">
                            <li class="active tab-sky">
                                <a data-toggle="tab" href="#home3">Home
                                </a>
                            </li>

                            <li class="tab-red">
                                <a data-toggle="tab" href="#profile3">Profile
                                </a>
                            </li>

                            <li class="tab-orange">
                                <a data-toggle="tab" href="#dropdown13">More
                                </a>
                            </li>
                        </ul>

                        <div class="tab-content">
                            <div id="home3" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile3" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="dropdown13" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>
                        </div>
                    </div>
                    <div class="horizontal-space"></div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable tabs-right">
                        <ul class="nav nav-tabs" id="myTab4">
                            <li class="active tab-green">
                                <a data-toggle="tab" href="#home4">Home</a>
                            </li>

                            <li class="tab-purple">
                                <a data-toggle="tab" href="#profile4">Profile</a>
                            </li>

                            <li class="tab-darkorange">
                                <a data-toggle="tab" href="#dropdown14">More</a>
                            </li>
                        </ul>

                        <div class="tab-content">
                            <div id="home4" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile4" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="dropdown14" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>
                        </div>
                    </div>
                    <div class="horizontal-space"></div>
                </div>
            </div>

            <h5 class="row-title before-palegreen"><i class="fa fa-align-justify palegreen"></i>Justified Tabs</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable">
                        <ul class="nav nav-tabs nav-justified" id="myTab5">
                            <li class="active">
                                <a data-toggle="tab" href="#home5">Home
                                </a>
                            </li>

                            <li class="tab-red">
                                <a data-toggle="tab" href="#profile5">Profile
                                </a>
                            </li>

                            <li class="dropdown">
                                <a data-toggle="dropdown" class="dropdown-toggle" href="#">Dropdown
                                                    <b class="caret"></b>
                                </a>

                                <ul class="dropdown-menu dropdown-blue">
                                    <li>
                                        <a data-toggle="tab" href="#dropdown5">@fat</a>
                                    </li>

                                    <li>
                                        <a data-toggle="tab" href="#dropdown15">@mdo</a>
                                    </li>
                                </ul>
                            </li>
                        </ul>

                        <div class="tab-content">
                            <div id="home5" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile5" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown5" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>

                            <div id="dropdown15" class="tab-pane">
                                <p>Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable tabs-below">
                        <div class="tab-content">
                            <div id="home6" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile6" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown16" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>
                        </div>

                        <ul class="nav nav-tabs nav-justified" id="myTab6">
                            <li class="active">
                                <a data-toggle="tab" href="#home6">Home</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#profile6">Profile</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#dropdown16">More</a>
                            </li>
                        </ul>
                    </div>
                    <div class="horizontal-space"></div>
                </div>
            </div>

            <h5 class="row-title before-yellow"><i class="glyphicon glyphicon-unchecked yellow"></i>Radius Bordered Tabs</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable">
                        <ul class="nav nav-tabs" id="myTab7">
                            <li class="active">
                                <a data-toggle="tab" href="#home7">Home
                                </a>
                            </li>

                            <li class="tab-red">
                                <a data-toggle="tab" href="#profile7">Profile
                                </a>
                            </li>

                            <li class="dropdown">
                                <a data-toggle="dropdown" class="dropdown-toggle" href="#">Dropdown
                                                    <b class="caret"></b>
                                </a>

                                <ul class="dropdown-menu dropdown-blue">
                                    <li>
                                        <a data-toggle="tab" href="#dropdown7">@fat</a>
                                    </li>

                                    <li>
                                        <a data-toggle="tab" href="#dropdown17">@mdo</a>
                                    </li>
                                </ul>
                            </li>
                        </ul>

                        <div class="tab-content radius-bordered">
                            <div id="home7" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile7" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown7" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>

                            <div id="dropdown17" class="tab-pane">
                                <p>Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="tabbable tabs-below">
                        <div class="tab-content radius-bordered">
                            <div id="home8" class="tab-pane in active">
                                <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                            </div>

                            <div id="profile8" class="tab-pane">
                                <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                            </div>

                            <div id="dropdown8" class="tab-pane">
                                <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                            </div>
                        </div>

                        <ul class="nav nav-tabs" id="myTab8">
                            <li class="active">
                                <a data-toggle="tab" href="#home8">Home</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#profile8">Profile</a>
                            </li>

                            <li>
                                <a data-toggle="tab" href="#dropdown8">More</a>
                            </li>
                        </ul>
                    </div>
                    <div class="horizontal-space"></div>
                </div>
            </div>

            <h5 class="row-title before-darkorange"><i class="glyphicon glyphicon-magnet darkorange"></i>Tabs In Containers</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="widget">
                        <div class="widget-header">
                            <span class="widget-caption">Tabs in Widget</span>
                        </div>

                        <div class="widget-body">
                            <div class="widget-main ">
                                <div class="tabbable">
                                    <ul class="nav nav-tabs" id="myTab9">
                                        <li class="active">
                                            <a data-toggle="tab" href="#home9">Home
                                            </a>
                                        </li>

                                        <li class="tab-red">
                                            <a data-toggle="tab" href="#profile9">Messages
                                            </a>
                                        </li>

                                        <li class="dropdown">
                                            <a data-toggle="dropdown" class="dropdown-toggle" href="#">Dropdown
                                                                <b class="caret"></b>
                                            </a>

                                            <ul class="dropdown-menu dropdown-info">
                                                <li>
                                                    <a data-toggle="tab" href="#dropdown9">@fat</a>
                                                </li>

                                                <li>
                                                    <a data-toggle="tab" href="#dropdown19">@mdo</a>
                                                </li>
                                            </ul>
                                        </li>
                                    </ul>

                                    <div class="tab-content">
                                        <div id="home9" class="tab-pane in active">
                                            <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                                        </div>

                                        <div id="profile9" class="tab-pane">
                                            <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                                        </div>

                                        <div id="dropdown9" class="tab-pane">
                                            <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                                        </div>

                                        <div id="dropdown19" class="tab-pane">
                                            <p>Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well with-header">
                        <div class="header bordered-darkorange">Tabs In Well</div>
                        <div class="tabbable">
                            <ul class="nav nav-tabs nav-justified" id="myTab10">
                                <li class="active">
                                    <a data-toggle="tab" href="#home10">Home
                                    </a>
                                </li>

                                <li class="tab-red">
                                    <a data-toggle="tab" href="#profile10">Messages
                                    </a>
                                </li>

                                <li class="dropdown">
                                    <a data-toggle="dropdown" class="dropdown-toggle" href="#">Dropdown
                                                        <b class="caret"></b>
                                    </a>

                                    <ul class="dropdown-menu dropdown-info">
                                        <li>
                                            <a data-toggle="tab" href="#dropdown10">@fat</a>
                                        </li>

                                        <li>
                                            <a data-toggle="tab" href="#dropdown20">@mdo</a>
                                        </li>
                                    </ul>
                                </li>
                            </ul>

                            <div class="tab-content">
                                <div id="home10" class="tab-pane in active">
                                    <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                                </div>

                                <div id="profile10" class="tab-pane">
                                    <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                                </div>

                                <div id="dropdown10" class="tab-pane">
                                    <p>Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade.</p>
                                </div>

                                <div id="dropdown20" class="tab-pane">
                                    <p>Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="widget flat radius-bordered">
                        <div class="widget-header bg-themeprimary">
                            <span class="widget-caption">Flat Tabs in Widget</span>
                        </div>

                        <div class="widget-body">
                            <div class="widget-main ">
                                <div class="tabbable">
                                    <ul class="nav nav-tabs tabs-flat" id="myTab11">
                                        <li class="active">
                                            <a data-toggle="tab" href="#home11">Home
                                            </a>
                                        </li>
                                        <li>
                                            <a data-toggle="tab" href="#profile11">Messages
                                            </a>
                                        </li>
                                    </ul>
                                    <div class="tab-content tabs-flat">
                                        <div id="home11" class="tab-pane in active">
                                            <p>Raw denim you probably haven't heard of them jean shorts Austin.</p>
                                        </div>

                                        <div id="profile11" class="tab-pane">
                                            <p>Food truck fixie locavore, accusamus mcsweeney's marfa nulla single-origin coffee squid.</p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                </div>
            </div>
        </div>
    </div>
    <div class="row">
        <div class="col-lg-12 col-sm-12 col-xs-12">
            <h5 class="row-title before-pink"><i class="glyphicon glyphicon-resize-vertical pink"></i>Accordions</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="widget">
                        <div class="widget-header bordered-bottom bordered-blue">
                            <span class="widget-caption">Accordion in Widget</span>

                            <div class="widget-buttons">
                                <a href="#" data-toggle="maximize">
                                    <i class="fa fa-expand"></i>
                                </a>
                                <a href="#" data-toggle="collapse">
                                    <i class="fa fa-minus"></i>
                                </a>
                                <a href="#" data-toggle="dispose">
                                    <i class="fa fa-times"></i>
                                </a>
                            </div>
                        </div>

                        <div class="widget-body no-padding">
                            <div class="widget-main ">
                                <div class="panel-group accordion" id="accordion">
                                    <div class="panel panel-default">
                                        <div class="panel-heading ">
                                            <h4 class="panel-title">
                                                <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion" href="#collapseOne">Collapsible Group Item #1
                                                </a>
                                            </h4>
                                        </div>
                                        <div id="collapseOne" class="panel-collapse collapse in">
                                            <div class="panel-body border-red">
                                                Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                            </div>
                                        </div>
                                    </div>
                                    <div class="panel panel-default">
                                        <div class="panel-heading">
                                            <h4 class="panel-title">
                                                <a class="accordion-toggle collapsed" data-toggle="collapse" data-parent="#accordion" href="#collapseTwo">Collapsible Group Item #2
                                                </a>
                                            </h4>
                                        </div>
                                        <div id="collapseTwo" class="panel-collapse collapse">
                                            <div class="panel-body border-palegreen">
                                                Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                            </div>
                                        </div>
                                    </div>
                                    <div class="panel panel-default">
                                        <div class="panel-heading">
                                            <h4 class="panel-title">
                                                <a class="accordion-toggle collapsed" data-toggle="collapse" data-parent="#accordion" href="#collapseThree">Collapsible Group Item #3
                                                </a>
                                            </h4>
                                        </div>
                                        <div id="collapseThree" class="panel-collapse collapse">
                                            <div class="panel-body border-magenta">
                                                Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="panel-group accordion" id="accordions">
                        <div class="panel panel-default">
                            <div class="panel-heading">
                                <h4 class="panel-title">
                                    <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordions" href="#collapseOnes">
                                        <i class="fa-fw fa fa-check"></i>Collapsible Group Item #1
                                    </a>
                                </h4>
                            </div>
                            <div id="collapseOnes" class="panel-collapse collapse in">
                                <div class="panel-body border-red">
                                    Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                </div>
                            </div>
                        </div>
                        <div class="panel panel-default">
                            <div class="panel-heading">
                                <h4 class="panel-title">
                                    <a class="accordion-toggle collapsed" data-toggle="collapse" data-parent="#accordions" href="#collapseTwos">Collapsible Group Item #2
                                    </a>
                                </h4>
                            </div>
                            <div id="collapseTwos" class="panel-collapse collapse">
                                <div class="panel-body border-red">
                                    Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                </div>
                            </div>
                        </div>
                        <div class="panel panel-default">
                            <div class="panel-heading">
                                <h4 class="panel-title">
                                    <a class="accordion-toggle collapsed" data-toggle="collapse" data-parent="#accordions" href="#collapseThrees">Collapsible Group Item #3
                                    </a>
                                </h4>
                            </div>
                            <div id="collapseThrees" class="panel-collapse collapse">
                                <div class="panel-body border-gold">
                                    Anim pariatur cliche reprehenderit, enim eiusmod high life accusamus terry richardson ad squid. 3 wolf moon officia aute, non cupidatat skateboard dolor brunch. Food truck quinoa nesciunt laborum eiusmod. Brunch 3 wolf moon tempor, sunt aliqua put a bird on it squid single-origin coffee nulla assumenda shoreditch et. Nihil anim keffiyeh helvetica, craft beer labore wes anderson cred nesciunt sapiente ea proident. Ad vegan excepteur butcher vice lomo. Leggings occaecat craft beer farm-to-table, raw denim aesthetic synth nesciunt you probably haven't heard of them accusamus labore sustainable VHS.
                                </div>
                            </div>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    </div>
</asp:Content>
<asp:Content ID="Scripts" ContentPlaceHolderID="PageScriptContent" runat="server">
    
</asp:Content>
