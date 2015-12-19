<%@ Page Title="Alerts and Notifications" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Alerts.aspx.cs" Inherits="BeyondAdmin.WebForms.Pages.Alerts" %>
<asp:Content ID="Main" ContentPlaceHolderID="MainContent" runat="server">
     <div class="row">
                        <div class="col-lg-12 col-sm-12 col-xs-12">
                            <h5 class="row-title"><i class="fa fa-warning themeprimary"></i>Alerts</h5>
                            <div class="row">
                                <div class="col-lg-6 col-sm-6 col-xs-12">
                                    <div class="well with-header">
                                        <div class="header bordered-darkorange">Default Alerts</div>
                                        <div class="alert alert-warning fade in">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <i class="fa-fw fa fa-warning"></i>
                                            <strong>Warning</strong> Your monthly traffic is reaching limit.
                                        </div>

                                        <div class="alert alert-success fade in">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <i class="fa-fw fa fa-check"></i>
                                            <strong>Success</strong> The page has been added.
                                        </div>

                                        <div class="alert alert-info fade in">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <i class="fa-fw fa fa-info"></i>
                                            <strong>Info!</strong> You have 8 unread messages.
                                        </div>

                                        <div class="alert alert-danger fade in">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <i class="fa-fw fa fa-times"></i>
                                            <strong>Error!</strong> Update has failed.
                                        </div>
                                    </div>

                                </div>
                                <div class="col-lg-6 col-sm-6 col-xs-12">
                                    <div class="well with-header">
                                        <div class="header bordered-blueberry">Rounded & Shadowed Alerts</div>
                                        <div class="alert alert-warning fade in radius-bordered alert-shadowed">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <span class="badge badge-success graded">
                                                0
                                            </span>
                                            <strong>Warning</strong> Your monthly traffic is reaching limit.
                                        </div>

                                        <div class="alert alert-success fade in radius-bordered alert-shadowed">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <span class="badge badge-darkorange graded">
                                                2
                                            </span>
                                            <strong>Success</strong> Two Tasks Are Completed.
                                        </div>

                                        <div class="alert alert-info fade in radius-bordered alert-shadowed">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <span class="badge badge-sky graded">
                                                8
                                            </span>
                                            <strong>Info!</strong> You have 8 unread messages.
                                        </div>

                                        <div class="alert alert-danger fade in radius-bordered alert-shadowed">
                                            <button class="close" data-dismiss="alert">
                                                ×
                                            </button>
                                            <i class="fa-fw fa fa-times"></i>
                                            <strong>Error!</strong> Update has failed.
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div class="row">
                                <div class="col-lg-6 col-sm-6 col-xs-12">
                                    <h5 class="row-title before-darkorange"><i class="fa fa-flash darkorange"></i>Tooltips</h5>
                                    <div class="well bordered-top bordered-darkorange">
                                        <h5>Inline Tooltips</h5>
                                        <p>
                                            Tight pants next level keffiyeh <a href="#" class="tooltips" data-toggle="tooltip" data-original-title="Default tooltip">you probably</a> haven't heard of them. Photo booth beard raw denim letterpress vegan messenger bag stumptown. Farm-to-table seitan, mcsweeney's fixie sustainable quinoa 8-bit american apparel <a href="#" class="tooltips tooltip-pink" data-toggle="tooltip" data-original-title="Another tooltip">have a</a> terry richardson vinyl chambray. <a href="#" class="tooltips tooltip-blueberry" title="" data-toggle="tooltip" data-original-title="12">twitter handle</a> freegan cred raw denim single-origin coffee viral.
                                        </p>
                                        <hr class="wide" />
                                        <h5>Tooltip Positions</h5>
                                        <div class="buttons-preview">
                                            <button class="btn btn-default" data-toggle="tooltip" data-placement="top" data-original-title="Tooltip in top">Top</button>
                                            <button class="btn btn-default" data-toggle="tooltip" data-placement="left" data-original-title="Tooltip in left">Left</button>
                                            <button class="btn btn-default" data-toggle="tooltip" data-placement="right" data-original-title="Tooltip in right">Right</button>
                                            <button class="btn btn-default" data-toggle="tooltip" data-placement="bottom" data-original-title="Tooltip in bottom">Bottom</button>
                                        </div>
                                        <hr class="wide" />
                                        <h5>Tooltip Colors</h5>
                                        <div class="buttons-preview">
                                            <button class="btn btn-danger tooltip-danger" data-toggle="tooltip" data-placement="top" data-original-title="Danger Tooltip">Danger</button>
                                            <button class="btn btn-warning tooltip-warning" data-toggle="tooltip" data-placement="top" data-original-title="Warning Tooltip">Warning</button>
                                            <button class="btn btn-info tooltip-info" data-toggle="tooltip" data-placement="top" data-original-title="Info Tooltip">Info</button>
                                            <button class="btn btn-success tooltip-success" data-toggle="tooltip" data-placement="top" data-original-title="Success Tooltip">Success</button>
                                            <button class="btn btn-blue tooltip-blue" data-toggle="tooltip" data-placement="top" data-original-title="Blue Tooltip">Blue</button>
                                            <button class="btn btn-palegreen tooltip-palegreen" data-toggle="tooltip" data-placement="top" data-original-title="Pale Green Tooltip">Pale Green</button>
                                            <button class="btn btn-darkorange tooltip-darkorange" data-toggle="tooltip" data-placement="top" data-original-title="Dark Orange Tooltip">Dark Orange</button>
                                            <button class="btn btn-magenta tooltip-magenta" data-toggle="tooltip" data-placement="top" data-original-title="Magenta Tooltip">Magenta</button>
                                            <button class="btn btn-purple tooltip-purple" data-toggle="tooltip" data-placement="top" data-original-title="Blue Tooltip">Purple</button>
                                            <button class="btn btn-maroon tooltip-maroon" data-toggle="tooltip" data-placement="top" data-original-title="Maroon Tooltip">Maroon</button>
                                        </div>
                                        <hr class="wide" />
                                        <h5>Tooltip Template & Size</h5>
                                        <div class="buttons-preview">

                                            <button class="btn btn-default" data-toggle="tooltip" data-placement="top" data-original-title="<img src='assets/img/avatars/divyia.jpg'>">With Image</button>
                                            <button class="btn btn-danger tooltip-lg" data-toggle="tooltip" data-placement="top" data-original-title="Large Tooltip">Large Tip</button>

                                        </div>
                                    </div>
                                </div>

                                <div class="col-lg-6 col-sm-6 col-xs-12">
                                    <h5 class="row-title before-blueberry"><i class="fa fa-flash blueberry"></i>Toastr Notifications</h5>
                                    <div class="well bordered-top bordered-blueberry">
                                        <h5>Notifications All Around The Page</h5>
                                        <div class="buttons-preview">
                                            <button class="btn btn-default" onclick="javascript: Notify('This is a Top-Left Notification.', 'top-left', '5000', 'danger', 'fa-desktop', true); return false;">Top Left</button>
                                            <button class="btn btn-default" onclick="javascript: Notify('This is a Top-Right Notification.', 'top-right', '5000', 'success', 'fa-list', true); return false;">Top Right</button>
                                            <button class="btn btn-default" onclick="javascript: Notify('This is a Bottom-Left Notification.', 'bottom-left', '5000', 'info', 'fa-tag', true); return false;">Bottom Left</button>
                                            <button class="btn btn-default" onclick="javascript: Notify('This is a Bottom-Right Notification.', 'bottom-right', '5000', 'warning', 'fa-edit', true); return false;">Bottom Right</button>
                                        </div>
                                        <hr class="wide" />
                                        <h5>Colored Notifications</h5>
                                        <div class="buttons-preview">
                                            <button class="btn btn-danger" onclick="javascript: Notify('Something Went Wrong!', 'top-right', '5000', 'danger', 'fa-bolt', true); return false;">Danger</button>
                                            <button class="btn btn-warning" onclick="javascript: Notify('You Must Worry', 'top-right', '5000', 'warning', 'fa-warning', true); return false;">Warning</button>
                                            <button class="btn btn-info" onclick="javascript: Notify('You`ve got mail.', 'top-right', '5000', 'info', 'fa-envelope', true); return false;">Info</button>
                                            <button class="btn btn-success" onclick="javascript: Notify('Everything is allright', 'top-right', '5000', 'success', 'fa-check', true); return false;">Success</button>
                                            <button class="btn btn-blue" onclick="javascript: Notify('Im a Blue notification', 'top-right', '5000', 'blue', 'fa-home', true); return false;">Blue</button>
                                            <button class="btn btn-palegreen" onclick="javascript: Notify('Im a Palegreen notification', 'top-right', '5000', 'palegreen', 'fa-home', true); return false;">PaleGreen</button>
                                            <button class="btn btn-darkorange" onclick="javascript: Notify('Im a Darkorange notification', 'top-right', '5000', 'darkorange', 'fa-home', true); return false;">Darkorange</button>
                                            <button class="btn btn-magenta" onclick="javascript: Notify('Im a Magenta notification', 'top-right', '5000', 'magenta', 'fa-home', true); return false;">Magenta</button>
                                            <button class="btn btn-purple" onclick="javascript: Notify('Im a Purple notification', 'top-right', '5000', 'purple', 'fa-home', true); return false;">Purple</button>
                                            <button class="btn btn-maroon" onclick="javascript: Notify('Im a Maroon notification', 'top-right', '5000', 'maroon', 'fa-home', true); return false;">Maroon</button>
                                        </div>
                                        <hr class="wide" />
                                        <h5>Notification With Html Content</h5>
                                        <div class="buttons-preview">
                                            <button class="btn btn-default" onclick="javascript: Notify($(imgdiv).html(), 'top-right', '5000', 'blueberry', 'fa-gift', true); return false;">With Html</button>
                                            <div id="imgdiv" style="display:none"><img width="24px" height="24px" src="assets/img/avatars/divyia.jpg"><span style="padding-left:10px;">John has sent you a gift</span></div>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <h5 class="row-title before-pink"><i class="fa fa-bullseye pink"></i>Pop Overs</h5>
                            <div class="row">
                                <div class="col-lg-12 col-sm-12 col-xs-12">
                                    <div class="well bordered-top bordered-purple popover-container">
                                        <div class="col-lg-12 col-sm-12 col-xs-12">
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover top dark">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-blue">Dark Popover top</h4>
                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover right">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-blueberry">Popover right</h4>
                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover bottom">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-palegreen">Popover bottom</h4>

                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover left">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-pink">Popover left</h4>
                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover top">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-warning">Popover</h4>
                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class="col-lg-4 col-sm-6 col-xs-12">
                                                <div class="popoverexample">
                                                    <div class="popover inverted top">
                                                        <div class="arrow"></div>
                                                        <h4 class="popover-title bordered-blue">Inverted Popover</h4>
                                                        <div class="popover-content">
                                                            <p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.</p>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                        <hr class="wide" />
                                        <h5>Popover On Buttons</h5>
                                        <div class="col-lg-12 col-sm-12 col-xs-12">
                                            <div class="buttons-preview text-align-center">
                                                <a href="javascript:void(0);" class="btn btn-default" data-container="body" data-titleclass="bordered-blue" data-class="" data-toggle="popover" data-placement="top" data-title="PopOver On Click" data-content="Vivamus sagittis lacus vel augue laoreet rutrum faucibus." data-original-title="" title="">
                                                    Popover on Click
                                                </a>
                                                <a href="javascript:void(0);" class="btn btn-default" data-container="body" data-titleclass="bordered-darkorange" data-toggle="popover" data-placement="top" data-title="Tabs in Popover" data-content="
                                                    <ul id='popup-tab' class='nav nav-tabs bordered'><li class='active'><a href='#pop-1' data-toggle='tab'>Tab1 </a></li><li><a href='#pop-2' data-toggle='tab'>Tab 2</a></li></ul><div id='popup-tab-content' class='tab-content padding-10'><div class='tab-pane fade in active' id='pop-1'><p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. </p></div><div class='tab-pane fade' id='pop-2'><p>Sed posuere consectetur est at lobortis. Aenean eu leo quam. </p></div></div>" data-original-title="" title="">
                                                    PopOver With Html Content
                                                </a>
                                                <a href="javascript:void(0);" class="btn btn-default" data-container="body" data-titleclass="bordered-purple" data-class="dark" data-toggle="popover-hover" data-placement="left" data-title="Hover" data-content="Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia quam venenatis vestibulum.." data-original-title="" title="">
                                                    Popover on Hover
                                                </a>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
</asp:Content>
<asp:Content ID="Scripts" ContentPlaceHolderID="PageScriptContent" runat="server">
        <script src="/assets/js/toastr/toastr.js"></script>
</asp:Content>
