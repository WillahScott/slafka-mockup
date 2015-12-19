<%@ Page Title="Modals and Wells" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="Modals.aspx.cs" Inherits="BeyondAdmin.WebForms.Pages.Modals" %>

<asp:Content ID="Main" ContentPlaceHolderID="MainContent" runat="server">
    <div class="row">
        <div class="col-lg-12 col-sm-12 col-xs-12">
            <h5 class="row-title before-darkorange"><i class="fa fa-reply-all darkorange"></i>Modal Dialoges</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well">
                        <div class="modal-preview">
                            <div class="modal modal-primary">
                                <div class="modal-dialog">
                                    <div class="modal-content">
                                        <div class="modal-header">
                                            <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
                                            <h4 class="modal-title">Modal Title</h4>
                                        </div>
                                        <div class="modal-body">
                                            <p>Modal Body</p>
                                        </div>
                                        <div class="modal-footer">
                                            <button type="button" class="btn btn-warning" data-dismiss="modal">Close</button>
                                            <button type="button" class="btn btn-primary">Save</button>
                                        </div>
                                    </div>
                                    <!-- /.modal-content -->
                                </div>
                                <!-- /.modal-dialog -->
                            </div>
                            <!-- /.modal -->
                        </div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well">
                        <div class="modal-preview">
                            <div class="modal modal-darkorange">
                                <div class="modal-dialog">
                                    <div class="modal-content">
                                        <div class="modal-header">
                                            <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
                                            <h4 class="modal-title">Modal Title</h4>
                                        </div>
                                        <div class="modal-body">
                                            <p>Modal Body</p>
                                        </div>
                                        <div class="modal-footer">
                                            <button type="button" class="btn btn-default" data-dismiss="modal">Cancel</button>
                                            <button type="button" class="btn btn-default">Ok</button>
                                        </div>
                                    </div>
                                    <!-- /.modal-content -->
                                </div>
                                <!-- /.modal-dialog -->
                            </div>
                            <!-- /.modal -->

                        </div>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well with-header">
                        <div class="header bordered-pink">Modals in action</div>
                        <div class="buttons-preview">
                            <h5>Modal Sizes</h5>
                            <button class="btn btn-default" data-toggle="modal" data-target=".bs-example-modal-sm">Small modal</button>
                            <button class="btn btn-default" data-toggle="modal" data-target=".bs-example-modal-lg">Large modal</button>
                            <hr class="wide" />
                            <h5>Bootbox Modal</h5>
                            <button class="btn btn-palegreen" id="bootbox-regular">Bootbox Dialog</button>
                            <button class="btn btn-sky" id="bootbox-confirm">Bootbox Confirm</button>
                            <button class="btn btn-darkorange" id="bootbox-options">Bootbox Custom Form</button>
                        </div>
                    </div>

                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well with-header">
                        <div class="header bordered-blue">Message Modals</div>
                        <div class="buttons-preview">
                            <button class="btn btn-success" data-toggle="modal" data-target="#modal-success">Success</button>
                            <button class="btn btn-info" data-toggle="modal" data-target="#modal-info">Info</button>
                            <button class="btn btn-danger" data-toggle="modal" data-target="#modal-danger">Danger</button>
                            <button class="btn btn-warning" data-toggle="modal" data-target="#modal-warning">Warning</button>
                        </div>
                    </div>
                </div>
            </div>

            <h5 class="row-title before-palegreen"><i class="fa fa-columns palegreen"></i>Wells</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well well-lg">
                        <h4 class="block">I am a large well</h4>
                        <p>
                            Tight pants next level keffiyeh trigger me on click haven't heard of them. Photo booth beard raw denim letterpress vegan messenger bag stumptown.
                        </p>
                    </div>
                    <div class="well">
                        <h4 class="block">I am a default well</h4>
                        <p>
                            Tight pants next level keffiyeh trigger me on click haven't heard of them. Photo booth beard raw denim letterpress vegan messenger bag stumptown.
                        </p>
                    </div>
                    <div class="well well-sm">
                        <h4 class="block">I am a small well</h4>
                        <p>
                            Tight pants next level keffiyeh trigger me on click haven't heard of them. Photo booth beard raw denim letterpress vegan messenger bag stumptown.
                        </p>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="bordered-well-container">
                        <div class="well bordered-top bordered-blue">
                            <div class="well bordered-top bordered-danger">
                                <div class="well bordered-top bordered-warning">
                                    <div class="well bordered-top bordered-palegreen">
                                        <div class="well bordered-top bordered-bottom bordered-pink">
                                            <h4 class="block">Bordered Wells</h4>
                                            <p>
                                                Tight pants next level keffiyeh trigger me on click haven't heard of them. Photo booth beard raw denim letterpress vegan messenger bag stumptown.
                                            </p>
                                        </div>

                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <h5 class="row-title before-maroon"><i class="fa fa-picture-o maroon"></i>Colored Wells</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well bg-magenta bordered-left bordered-yellow">
                        <h4 class="block">Colored Well</h4>
                        <p>
                            With Border on Left
                        </p>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well bg-palegreen bordered-right bordered-success">
                        <h4 class="block">Colored Well</h4>
                        <p>
                            With Border on Right
                        </p>
                    </div>
                </div>
            </div>
            <h5 class="row-title before-blueberry"><i class="typcn typcn-th-menu blueberry"></i>Wells With Header and Footer</h5>
            <div class="row">
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well with-header">
                        <div class="header bordered-magenta">Well With Header</div>
                    </div>
                </div>
                <div class="col-lg-6 col-sm-6 col-xs-12">
                    <div class="well with-header with-footer">
                        <div class="header bordered-palegreen">Well With Header & Footer</div>
                        <div class="footer">I'm Footer</div>
                    </div>
                </div>
            </div>

            <h5 class="row-title before-orange"><i class="typcn typcn-th-menu orange"></i>Attached Wells</h5>
            <div class="row">
                <div class="col-xs-12">
                    <div class="well attached top">
                        Top Well
                    </div>
                    <div class="well attached">
                        Middle Well
                    </div>
                    <div class="well attached bottom">
                        Bottom Well
                    </div>
                </div>
            </div>
        </div>
    </div>
    <!--Modal Templates-->
    <!--Email Modal Templates-->
    <div id="myModal" style="display: none;">
        <div class="row">
            <div class="col-md-12">
                <div class="form-group">
                    <input type="text" class="form-control" placeholder="To" required="">
                </div>
                <div class="form-group">
                    <input type="text" class="form-control" placeholder="Subject" required="">
                </div>
                <div class="form-group">
                    <textarea class="form-control" placeholder="Content" rows="5" required=""></textarea>
                </div>
            </div>
        </div>
    </div>
    <!--End Email Templates-->
    <!--Success Modal Templates-->
    <div id="modal-success" class="modal modal-message modal-success fade" style="display: none;" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <i class="glyphicon glyphicon-check"></i>
                </div>
                <div class="modal-title">Success</div>

                <div class="modal-body">You have done great!</div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-success" data-dismiss="modal">OK</button>
                </div>
            </div>
            <!-- / .modal-content -->
        </div>
        <!-- / .modal-dialog -->
    </div>
    <!--End Success Modal Templates-->
    <!--Info Modal Templates-->
    <div id="modal-info" class="modal modal-message modal-info fade" style="display: none;" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <i class="fa fa-envelope"></i>
                </div>
                <div class="modal-title">Alert</div>

                <div class="modal-body">You'vd got mail!</div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-info" data-dismiss="modal">OK</button>
                </div>
            </div>
            <!-- / .modal-content -->
        </div>
        <!-- / .modal-dialog -->
    </div>
    <!--End Info Modal Templates-->
    <!--Danger Modal Templates-->
    <div id="modal-danger" class="modal modal-message modal-danger fade" style="display: none;" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <i class="glyphicon glyphicon-fire"></i>
                </div>
                <div class="modal-title">Alert</div>

                <div class="modal-body">You'vd done bad!</div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-danger" data-dismiss="modal">OK</button>
                </div>
            </div>
            <!-- / .modal-content -->
        </div>
        <!-- / .modal-dialog -->
    </div>
    <!--End Danger Modal Templates-->
    <!--Danger Modal Templates-->
    <div id="modal-warning" class="modal modal-message modal-warning fade" style="display: none;" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <i class="fa fa-warning"></i>
                </div>
                <div class="modal-title">Warning</div>

                <div class="modal-body">Is something wrong?</div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-warning" data-dismiss="modal">OK</button>
                </div>
            </div>
            <!-- / .modal-content -->
        </div>
        <!-- / .modal-dialog -->
    </div>
    <!--End Danger Modal Templates-->
    <!--LArge Modal Templates-->
    <div class="modal fade bs-example-modal-lg" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel" aria-hidden="true" style="display: none;">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">

                <div class="modal-header">
                    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
                    <h4 class="modal-title" id="myLargeModalLabel">Large modal</h4>
                </div>
                <div class="modal-body">
                    ...
                </div>
            </div>
            <!-- /.modal-content -->
        </div>
        <!-- /.modal-dialog -->
    </div>
    <!--End Large Modal Templates-->
    <!--Small Modal Templates-->
    <div class="modal fade bs-example-modal-sm" tabindex="-1" role="dialog" aria-labelledby="mySmallModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-sm">
            <div class="modal-content">

                <div class="modal-header">
                    <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
                    <h4 class="modal-title" id="mySmallModalLabel">Small modal</h4>
                </div>
                <div class="modal-body">
                    ...
                </div>
            </div>
            <!-- /.modal-content -->
        </div>
        <!-- /.modal-dialog -->
    </div>
    <!--End Small Modal Templates-->
    <!--End Modal Templates-->
</asp:Content>
<asp:Content ID="Scripts" ContentPlaceHolderID="PageScriptContent" runat="server">
    <script src="/assets/js/bootbox/bootbox.js"></script>
    <script>
        $("#bootbox-regular").on('click', function () {
            bootbox.prompt("What is your name?", function (result) {
                if (result === null) {
                    //Example.show("Prompt dismissed");
                } else {
                    //Example.show("Hi <b>"+result+"</b>");
                }
            });
        });

        $("#bootbox-confirm").on('click', function () {
            bootbox.confirm("Are you sure?", function (result) {
                if (result) {
                    //
                }
            });
        });

        $("#bootbox-options").on('click', function () {
            bootbox.dialog({
                message: $("#myModal").html(),
                title: "New E-Mail",
                className: "modal-darkorange",
                buttons: {
                    success: {
                        label: "Send",
                        className: "btn-blue",
                        callback: function () { }
                    },
                    "Save as Draft": {
                        className: "btn-danger",
                        callback: function () { }
                    }
                }
            });
        });

        $("#bootbox-success").on('click', function () {
            bootbox.dialog({
                message: $("#modal-success").html(),
                title: "Success",
                className: "",
            });
        });
    </script>
</asp:Content>
