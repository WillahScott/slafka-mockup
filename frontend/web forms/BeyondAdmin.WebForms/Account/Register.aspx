<%@ Page Title="Register" Language="C#" MasterPageFile="~/Empty.Master" AutoEventWireup="true" CodeBehind="Register.aspx.cs" Inherits="BeyondAdmin.WebForms.Account.Register" %>

<asp:Content runat="server" ID="BodyContent" ContentPlaceHolderID="MainContent">
     <div class="register-container animated fadeInDown" >
        <div class="registerbox bg-white" style="height: 570px !important;">
            <div class="registerbox-title">Register</div>

            <div class="registerbox-caption ">Please fill in your information</div>
            <div class="registerbox-textbox">
                <asp:TextBox runat="server" ID="Email" CssClass="form-control" TextMode="Email" placeholder="Email" />
                <asp:RequiredFieldValidator runat="server" ControlToValidate="Email"
                    CssClass="text-danger" ErrorMessage="The email field is required." />
            </div>
            <div class="registerbox-textbox">
                <asp:TextBox runat="server" ID="Password" TextMode="Password" CssClass="form-control" placeholder="Enter Password"/>
                <asp:RequiredFieldValidator runat="server" ControlToValidate="Password"
                    CssClass="text-danger" ErrorMessage="The password field is required." />
            </div>
            <div class="registerbox-textbox">
                <asp:TextBox runat="server" ID="ConfirmPassword" TextMode="Password" CssClass="form-control" placeholder="Confirm Password"/>
                <asp:RequiredFieldValidator runat="server" ControlToValidate="ConfirmPassword"
                    CssClass="text-danger" Display="Dynamic" ErrorMessage="The confirm password field is required." />
                <asp:CompareValidator runat="server" ControlToCompare="Password" ControlToValidate="ConfirmPassword"
                    CssClass="text-danger" Display="Dynamic" ErrorMessage="The password and confirmation password do not match." />
            </div>
            <hr class="wide" />
            <div class="registerbox-textbox">
                <input type="text" class="form-control" placeholder="Name" />
            </div>
            <div class="registerbox-textbox">
                <input type="text" class="form-control" placeholder="Family" />
            </div>
            <div class="registerbox-textbox">
                <div class="row">
                    <div class="col-lg-6 col-sm-6 col-xs-6 padding-right-10">
                        <input type="text" class="form-control" placeholder="Month" />
                    </div>
                    <div class="col-lg-3 col-sm-3 col-xs-3 no-padding padding-right-10">
                        <input type="text" class="form-control" placeholder="Day" />
                    </div>
                    <div class="col-lg-3 col-sm-3 col-xs-3 no-padding-left">
                        <input type="text" class="form-control" placeholder="Year" />
                    </div>
                </div>
            </div>
            <div class="registerbox-textbox no-padding-bottom">
                <div class="checkbox">
                    <label>
                        <input type="checkbox" class="" checked="checked">
                        <span class="text darkgray">I agree to the <a class="themeprimary">Terms of Service</a> </span>
                    </label>
                </div>
            </div>
            <div class="registerbox-submit">
                <asp:Button runat="server" OnClick="CreateUser_Click" Text="Register" CssClass="btn btn-primary pull-right" />
            </div>
        </div>
        <div class="logobox">
        </div>
    </div>
    <p class="text-danger">
        <asp:Literal runat="server" ID="ErrorMessage" />
    </p>

</asp:Content>
